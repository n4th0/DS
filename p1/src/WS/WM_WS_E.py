import argparse
import os
import socket
import sys
import threading
import time
from datetime import datetime

from protocol import send_frame, recv_frame, pack_message, unpack_message

try:
    import msvcrt
except ImportError:
    msvcrt = None


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


class WSEngine:
    def __init__(self, kafka_broker: str, monitor_ip: str, monitor_port: int):
        self.kafka_broker = kafka_broker
        self.monitor_ip = monitor_ip
        self.monitor_port = monitor_port

        self.fault = False  # se activa con 'k', el engine responde KO a los pings
        self.fault_lock = threading.Lock()

        self.watering = False
        self.flow_lpm = 0.0
        self.accumulated_l = 0.0

    # conexion
    def connect_to_monitor(self) -> socket.socket:
        while True:
            try:
                sock = socket.create_connection((self.monitor_ip, self.monitor_port), timeout=5)
                log(f"Conectado al Monitor en {self.monitor_ip}:{self.monitor_port}")
                return sock
            except OSError as exc:
                log(f"Monitor inalcanzable ({exc}); reintentando en 3 segundos...")
                time.sleep(3)

    # simulacion de averia 
    def keyboard_listener(self) -> None:
        if msvcrt is not None:
            log("Pulsa 'k' para simular una averia (KO), 'q' para salir.")
            while True:
                if msvcrt.kbhit():
                    key = msvcrt.getch().decode(errors="ignore").lower()
                    self._handle_key(key)
                time.sleep(0.2)
        else:
            log("Escribe 'k' + Enter para simular una averia (KO), 'q' + Enter para salir.")
            while True:
                line = sys.stdin.readline()
                if not line:
                    return
                self._handle_key(line.strip().lower())

    def _handle_key(self, key: str) -> None:
        if key == "k":
            with self.fault_lock:
                self.fault = not self.fault
            log(f"Averia simulada {'ACTIVADA' if self.fault else 'DESACTIVADA'}")
        elif key == "q":
            log("Apagando el Engine.")
            os._exit(0)

    # riego (se completara cuando existan CENTRAL/FO) 
    def start_watering(self, operator_id: str, time_limit_s: int) -> None:
        raise NotImplementedError("se conectara cuando CENTRAL pueda solicitar riego")

    def stop_watering(self) -> None:
        raise NotImplementedError("se conectara cuando CENTRAL pueda solicitar riego")

    # protocolo con el Monitor
    def handle_monitor(self, sock: socket.socket) -> None:
        while True:
            request = recv_frame(sock)
            if request is None:
                log("El Monitor cerro la conexion")
                return
            op, _ = unpack_message(request)
            if op == "PING":
                with self.fault_lock:
                    faulty = self.fault
                send_frame(sock, pack_message("KO" if faulty else "OK"))
            else:
                log(f"Peticion no reconocida del Monitor: {request}")

    def run(self) -> None:
        threading.Thread(target=self.keyboard_listener, daemon=True).start()
        while True:
            sock = self.connect_to_monitor()
            self.handle_monitor(sock)
            log("Reconectando con el Monitor...")


def main() -> None:
    parser = argparse.ArgumentParser(description="WM_WS_E - Engine de la estacion de riego")
    parser.add_argument("kafka_broker", help="host:puerto del broker/bootstrap-server de Kafka (se usara mas adelante)")
    parser.add_argument("monitor_ip", help="IP del Monitor de esta WS")
    parser.add_argument("monitor_port", type=int, help="puerto del Monitor de esta WS")
    args = parser.parse_args()

    engine = WSEngine(args.kafka_broker, args.monitor_ip, args.monitor_port)
    engine.run()


if __name__ == "__main__":
    main()