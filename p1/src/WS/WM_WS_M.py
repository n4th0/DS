import argparse
import socket
import threading
import time
from datetime import datetime

from protocol import send_frame, recv_frame, pack_message, unpack_message

HEALTH_CHECK_PERIOD = 1.0 # el enunciado pide comprobar el estado "cada segundo"
HEALTH_CHECK_TIMEOUT = 2.0  # segundos de espera para la respuesta del Engine
CENTRAL_RETRY_PERIOD = 5.0  # segundos entre reintentos de conexion a CENTRAL

STATE_DISCONNECTED = "DESCONECTADA"
STATE_AVAILABLE = "DISPONIBLE"
STATE_WATERING = "REGANDO"
STATE_LEAK = "FUGA"
STATE_OUT_OF_SERVICE = "FUERA_DE_SERVICIO"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


class WSMonitor:
    def __init__(self, monitor_port: int, central_ip: str, central_port: int, ws_id: str):
        self.monitor_port = monitor_port
        self.central_ip = central_ip
        self.central_port = central_port
        self.ws_id = ws_id

        self.state = STATE_DISCONNECTED
        self.state_lock = threading.Lock()

        self.central_conn: socket.socket | None = None
        self.central_lock = threading.Lock()

    # estado
    def set_state(self, new_state: str) -> None:
        with self.state_lock:
            if self.state != new_state:
                log(f"WS-{self.ws_id} cambia de estado {self.state} -> {new_state}")
                self.state = new_state

    # conexion con CENTRAL
    def central_worker(self) -> None:   
        while True:
            try:
                sock = socket.create_connection(
                    (self.central_ip, self.central_port), timeout=3
                )
                with self.central_lock:
                    self.central_conn = sock
                log(f"Conectado a CENTRAL en {self.central_ip}:{self.central_port}")

                send_frame(sock, pack_message("REGISTER_WS", self.ws_id))
                answer = recv_frame(sock, timeout=5)
                if answer:
                    op, _ = unpack_message(answer)
                    if op == "REGISTER_OK":
                        self.set_state(STATE_AVAILABLE)
                        log(f"WS-{self.ws_id} registrada/autenticada en CENTRAL")
                    else:
                        log(f"CENTRAL rechazo el registro: {answer}")

                while True:
                    msg = recv_frame(sock)
                    if msg is None:
                        raise ConnectionError("CENTRAL cerro la conexion")
                    # TODO siguiente iteracion: procesar ordenes de CENTRAL
                    # (Bloquear WS / Activar WS / Iniciar riego)
                    log(f"Mensaje de CENTRAL: {msg}")

            except (ConnectionError, OSError) as exc:
                log(f"CENTRAL inalcanzable ({exc}); WS-{self.ws_id} -> {STATE_DISCONNECTED}")
                with self.central_lock:
                    self.central_conn = None
                self.set_state(STATE_DISCONNECTED)
                time.sleep(CENTRAL_RETRY_PERIOD)

    def notify_central(self, op_code: str, *fields) -> None:
        with self.central_lock:
            sock = self.central_conn
        if sock is None:
            log(f"No se puede notificar a CENTRAL ({op_code}): sin conexion")
            return
        try:
            send_frame(sock, pack_message(op_code, self.ws_id, *fields))
        except OSError as exc:
            log(f"Fallo al notificar a CENTRAL: {exc}")

    # conexion con el Engine
    def accept_engine(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", self.monitor_port))
        server.listen(1)
        log(f"Monitor a la escucha del Engine en el puerto {self.monitor_port}")
        while True:
            conn, addr = server.accept()
            log(f"Engine conectado desde {addr}")
            self.health_check_loop(conn)
            log("Se perdio la conexion con el Engine, esperando reconexion")

    def health_check_loop(self, conn: socket.socket) -> None:
        while True:
            try:
                send_frame(conn, pack_message("PING"))
                answer = recv_frame(conn, timeout=HEALTH_CHECK_TIMEOUT)
                if answer is None:
                    raise ConnectionError("sin respuesta al ping de salud")

                op, _ = unpack_message(answer)
                if op == "OK":
                    if self.state == STATE_LEAK:
                        log(f"WS-{self.ws_id} se ha recuperado, fuga resuelta")
                        self.set_state(STATE_AVAILABLE)
                        self.notify_central("LEAK_RESOLVED")
                    elif self.state == STATE_DISCONNECTED:
                        self.set_state(STATE_AVAILABLE)
                elif op == "KO":
                    self.report_leak()
                else:
                    log(f"Respuesta inesperada del Engine: {answer}")

            except (ConnectionError, OSError):
                log(f"Se perdio el contacto con el Engine de WS-{self.ws_id}")
                self.report_leak()
                return

            time.sleep(HEALTH_CHECK_PERIOD)

    def report_leak(self) -> None:
        if self.state != STATE_LEAK:
            log(f"WS-{self.ws_id} FUGA detectada")
            self.set_state(STATE_LEAK)
            self.notify_central("LEAK_DETECTED")

    def run(self) -> None:
        threading.Thread(target=self.central_worker, daemon=True).start()
        self.accept_engine()


def main() -> None:
    parser = argparse.ArgumentParser(description="WM_WS_M - Monitor de la estacion de riego")
    parser.add_argument("monitor_port", type=int, help="puerto en el que este Monitor escucha al Engine")
    parser.add_argument("central_ip", help="host/IP de CENTRAL")
    parser.add_argument("central_port", type=int, help="puerto de CENTRAL")
    parser.add_argument("ws_id", help="identificador unico de esta estacion de riego")
    args = parser.parse_args()

    monitor = WSMonitor(args.monitor_port, args.central_ip, args.central_port, args.ws_id)
    monitor.run()


if __name__ == "__main__":
    main()