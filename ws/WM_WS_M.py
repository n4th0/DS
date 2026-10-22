#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WM_WS_M - Monitor de la Watering Station.

* Se autentica/registra en WM_Central por sockets (STX/ETX/LRC) y mantiene la sesión con latidos.
* Ofrece un servidor de sockets al Engine (WM_WS_E): le entrega la identidad (ID + token de sesión)
  y le pregunta cada segundo por su salud (HEALTH). Si el Engine contesta KO o no contesta,
  comunica la AVERÍA (fuga) a CENTRAL; cuando se recupera, comunica la reparación.

Uso:
    python WM_WS_M.py --engine-port 7001 --central IP:PUERTO --id WS-01 --location "Central Park"
"""
import argparse
import os
import socket
import sys
import threading
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, ".."))

from wm_common import protocol as proto      # noqa: E402
from wm_common.util import env, parse_hostport, say  # noqa: E402


class Monitor:
    def __init__(self, ws_id, location, secret, central_addr, engine_port):
        self.ws_id = ws_id
        self.location = location.replace("#", " ")
        self.secret = secret
        self.central_addr = central_addr
        self.engine_port = engine_port
        self.lock = threading.Lock()
        self.running = True
        self.token = None            # token de sesión entregado por CENTRAL
        self.leak = False            # avería detectada localmente
        self.reported_leak = None    # lo último que CENTRAL sabe
        self.engine_conn = None
        self.engine_connected = False
        self.srv = None

    def say(self, msg):
        say(f"MONITOR {self.ws_id}", msg)

    def start(self):
        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("0.0.0.0", self.engine_port))
        self.srv.listen(5)
        self.engine_port = self.srv.getsockname()[1]
        self.say(f"Escuchando al Engine en el puerto {self.engine_port}")
        threading.Thread(target=self._engine_accept_loop, daemon=True).start()
        threading.Thread(target=self._central_loop, daemon=True).start()

    def stop(self):
        self.running = False
        for s in (self.srv, self.engine_conn):
            try:
                s.close()
            except Exception:
                pass

    # ------------------------------------------------------------ enlace con CENTRAL
    def _central_loop(self):
        while self.running:
            ch = None
            try:
                sock = socket.create_connection(self.central_addr, timeout=5)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                ch = proto.Channel(sock, timeout=5)
                proto.client_handshake(ch)
                ans = proto.request(ch, f"REG#{self.ws_id}#{self.location}#{self.secret}")
                parts = ans.split("#")
                if parts[0] != "REG_OK":
                    self.say(f"CENTRAL rechazó el registro: {ans}")
                    raise proto.ProtocolError(ans)
                with self.lock:
                    self.token = parts[1]
                    self.reported_leak = False
                self.say(f"Autenticado en CENTRAL {self.central_addr[0]}:{self.central_addr[1]}")
                last_hb = time.time()
                while self.running:
                    with self.lock:
                        want = self.leak
                        token = self.token
                    if want != self.reported_leak:
                        op = "LEAK" if want else "FIX"
                        ans = proto.request(ch, f"{op}#{self.ws_id}#{token}")
                        if ans != "OK":
                            raise proto.ProtocolError(f"CENTRAL respondió {ans}")
                        self.reported_leak = want
                        self.say("AVERÍA notificada a CENTRAL" if want else "Reparación notificada a CENTRAL")
                    elif time.time() - last_hb >= 2:
                        ans = proto.request(ch, f"HB#{self.ws_id}#{token}")
                        if ans != "HB_OK":
                            raise proto.ProtocolError(f"latido rechazado: {ans}")
                        last_hb = time.time()
                    time.sleep(0.2)
            except (proto.ProtocolError, OSError) as e:
                if self.running:
                    self.say(f"Sin enlace con CENTRAL ({e}); reintentando en 3 s")
            finally:
                with self.lock:
                    self.token = None
                if ch:
                    proto.send_eot(ch)
                    ch.close()
            for _ in range(15):
                if not self.running:
                    return
                time.sleep(0.2)

    # ------------------------------------------------------------ enlace con el Engine
    def _engine_accept_loop(self):
        while self.running:
            try:
                conn, addr = self.srv.accept()
            except OSError:
                return
            old, self.engine_conn = self.engine_conn, conn
            if old:
                try:
                    old.close()
                except OSError:
                    pass
            threading.Thread(target=self._engine_session, args=(conn, addr), daemon=True).start()

    def _set_health(self, ok, why=""):
        with self.lock:
            if not ok and not self.leak:
                self.leak = True
                self.say(f"AVERÍA detectada: {why}")
            elif ok and self.leak:
                self.leak = False
                self.say("Engine OK: avería resuelta")

    def _engine_session(self, conn, addr):
        ch = proto.Channel(conn, timeout=3.0)
        try:
            proto.accept_handshake(ch)
            msg = proto.recv_request(ch)
            with self.lock:
                token = self.token
            if not msg.startswith("HELLO"):
                proto.send_answer(ch, "HELLO_KO#BAD_REQUEST")
                return
            if not token:
                proto.send_answer(ch, "HELLO_KO#NOT_REGISTERED")
                self.say("Engine rechazado: este Monitor aún no está autenticado en CENTRAL")
                return
            proto.send_answer(ch, f"HELLO_OK#{self.ws_id}#{token}")
            self.engine_connected = True
            self.say(f"Engine conectado desde {addr[0]}:{addr[1]}")
            while self.running and self.engine_conn is conn:
                t0 = time.time()
                with self.lock:
                    token = self.token or "-"
                ans = proto.request(ch, f"HEALTH#{self.ws_id}#{token}")
                self._set_health(ans == "OK", "el Engine reporta KO")
                time.sleep(max(0.0, 1.0 - (time.time() - t0)))
        except (proto.ProtocolError, OSError) as e:
            if self.engine_conn is conn:
                self._set_health(False, f"el Engine no responde ({e})")
        finally:
            if self.engine_conn is conn:
                self.engine_connected = False
                self.engine_conn = None
            ch.close()


def main():
    ap = argparse.ArgumentParser(prog="WM_WS_M", description="Monitor de la estación de riego")
    ap.add_argument("--engine-port", type=int, default=int(env("WM_ENGINE_PORT", 7001)),
                    help="puerto del servidor de sockets que se ofrece a WM_WS_E")
    ap.add_argument("--central", default=env("WM_CENTRAL", "localhost:9000"), help="IP:puerto de WM_Central")
    ap.add_argument("--id", default=env("WM_WS_ID"), required=env("WM_WS_ID") is None,
                    help="identificador único de la estación (p. ej. WS-01)")
    ap.add_argument("--location", default=env("WM_WS_LOCATION", "Sin ubicación"), help="parque/jardín/zona")
    ap.add_argument("--secret", default=env("WM_WS_SECRET", "changeme"), help="secreto de autenticación de la WS")
    args = ap.parse_args()
    m = Monitor(args.id, args.location, args.secret, parse_hostport(args.central), args.engine_port)
    m.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        m.stop()


if __name__ == "__main__":
    main()
