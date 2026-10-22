#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WM_WS_E - Engine de la Watering Station (caudalímetro + electroválvula simulados).

* Conecta por sockets con su Monitor (WM_WS_M): recibe su ID y token de sesión (validados por CENTRAL)
  y responde cada segundo a las comprobaciones de salud (OK/KO).
* Recibe órdenes de CENTRAL por Kafka (START/STOP/BLOCK/UNBLOCK) y envía telemetría
  (STATUS cada 2 s, TICK cada segundo regando, STARTED, ENDED).
* Menú por teclado (stdin):  s [seg] = regar manualmente | e = finalizar riego |
  k = simular/quitar FUGA (KO al monitor) | h = ayuda | q = salir.

Uso:
    python WM_WS_E.py --broker IP:PUERTO --monitor IP:PUERTO
"""
import argparse
import os
import random
import socket
import sys
import threading
import time
import traceback
import uuid

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, ".."))

from wm_common import protocol as proto                    # noqa: E402
from wm_common import topics as T                          # noqa: E402
from wm_common.bus import get_bus                          # noqa: E402
from wm_common.util import env, is_stale, parse_hostport, say  # noqa: E402

HELP = ("Comandos:  s [segundos] = regar manualmente | e = finalizar riego | "
        "k = simular/quitar FUGA | h = ayuda | q = salir")


class Engine:
    def __init__(self, bus, monitor_addr, base_flow=12.0, default_duration=30, ttl=60.0, grace=1.0):
        self.bus = bus
        self.monitor_addr = monitor_addr
        self.base_flow = base_flow
        self.default_duration = default_duration
        self.ttl = ttl
        self.grace = grace
        self.lock = threading.RLock()
        self.running = True
        self.ws_id = None
        self.token = None
        self.link_ok = False
        self.subscribed = False
        self.ko = False
        self.blocked = False
        self.watering = None

    def say(self, msg):
        say(f"ENGINE {self.ws_id or '?'}", msg)

    # ------------------------------------------------------------------ arranque
    def start(self):
        threading.Thread(target=self._link_loop, daemon=True).start()
        threading.Thread(target=self._status_loop, daemon=True).start()

    def stop(self):
        self.running = False

    @property
    def state(self):
        if self.ko:
            return "KO"
        if self.blocked:
            return "BLOCKED"
        return "WATERING" if self.watering else "IDLE"

    # ------------------------------------------------------------------ enlace con el Monitor
    def _link_loop(self):
        while self.running:
            try:
                self._session()
            except (proto.ProtocolError, OSError) as e:
                self.say(f"Enlace con el Monitor perdido: {e}")
            except Exception:
                self.say("Error inesperado en el enlace:\n" + traceback.format_exc())
            self.link_ok = False
            with self.lock:
                w = self.watering
            if w:
                self.say("Monitor perdido: cierro la electroválvula por seguridad")
                self._end(w, "MONITOR_LOST")
            for _ in range(10):
                if not self.running:
                    return
                time.sleep(0.2)

    def _session(self):
        sock = socket.create_connection(self.monitor_addr, timeout=5)
        ch = proto.Channel(sock, timeout=5)
        try:
            proto.client_handshake(ch)
            ans = proto.request(ch, "HELLO#WM_WS_E")
            parts = ans.split("#")
            if parts[0] != "HELLO_OK" or len(parts) < 3:
                raise proto.ProtocolError(f"el Monitor no está listo ({ans})")
            self.ws_id, self.token = parts[1], parts[2]
            self.say("Identidad recibida del Monitor (validada por CENTRAL)")
            self._ensure_subscribed()
            self.link_ok = True
            self._send_status()
            while self.running:
                msg = proto.recv_request(ch)       # el Monitor pregunta; timeout => Monitor perdido
                p = msg.split("#")
                if p[0] == "HEALTH":
                    if len(p) > 2 and p[2] != "-":
                        self.token = p[2]          # CENTRAL pudo haber renovado el token
                    proto.send_answer(ch, "KO" if self.ko else "OK")
                else:
                    proto.send_answer(ch, "ERR#UNKNOWN_OP")
        finally:
            ch.close()

    def _ensure_subscribed(self):
        if self.subscribed:
            return
        ready = self.bus.subscribe([T.WS_COMMANDS], self._on_command,
                                   group_id=f"ws-engine-{self.ws_id}-{uuid.uuid4().hex[:6]}")
        ready.wait(30)
        time.sleep(self.grace)
        self.subscribed = True
        self.say("Suscrito a las órdenes de CENTRAL (Kafka)")

    # ------------------------------------------------------------------ telemetría
    def _tele(self, type_, **kw):
        if not self.ws_id:
            return
        self.bus.publish(T.WS_TELEMETRY, self.ws_id,
                         {"type": type_, "ws_id": self.ws_id, "token": self.token, **kw})

    def _send_status(self):
        self._tele("STATUS", engine_state=self.state)

    def _status_loop(self):
        while self.running:
            if self.link_ok and self.subscribed:
                self._send_status()
            time.sleep(2)

    # ------------------------------------------------------------------ órdenes de CENTRAL
    def _on_command(self, topic, key, m):
        if m.get("ws_id") != self.ws_id or is_stale(m, self.ttl):
            return
        cmd = m.get("cmd")
        if cmd == "START":
            self._cmd_start(m)
        elif cmd == "STOP":
            with self.lock:
                w = self.watering
            if w:
                self._end(w, m.get("reason", "STOPPED"))
        elif cmd == "BLOCK":
            with self.lock:
                self.blocked = True
                w = self.watering
            if w:
                self._end(w, m.get("reason", "BLOCKED_BY_CENTRAL"))
            self.say("FUERA DE SERVICIO por orden de CENTRAL")
            self._send_status()
        elif cmd == "UNBLOCK":
            with self.lock:
                self.blocked = False
            self.say("ACTIVADA por orden de CENTRAL (disponible)")
            self._send_status()

    def _cmd_start(self, m):
        req = m.get("req_id")
        op = m.get("operator_id", "?")
        try:
            dur = int(m.get("duration_s") or self.default_duration)
        except (TypeError, ValueError):
            dur = self.default_duration
        with self.lock:
            reason = "LEAK" if self.ko else "BLOCKED" if self.blocked else "BUSY" if self.watering else None
            if reason:
                self.say(f"Riego {req} DENEGADO localmente ({reason})")
                self._tele("START_DENIED", req_id=req, reason=reason)
                return
            w = {"req_id": req, "operator_id": op, "duration": dur, "volume": 0.0,
                 "started": time.time(), "stop": threading.Event()}
            self.watering = w
        self.say(f"Electroválvula ABIERTA: riego {req} solicitado por {op}, máx. {dur} s")
        self._tele("STARTED", req_id=req, operator_id=op, duration_s=dur)
        threading.Thread(target=self._watering_loop, args=(w,), daemon=True).start()

    # ------------------------------------------------------------------ riego
    def _watering_loop(self, w):
        while True:
            w["stop"].wait(1.0)
            if w["stop"].is_set():
                return
            flow = max(0.0, random.gauss(self.base_flow, 0.6))
            w["volume"] += flow / 60.0
            elapsed = time.time() - w["started"]
            self._tele("TICK", req_id=w["req_id"], operator_id=w["operator_id"],
                       flow_lpm=round(flow, 2), volume_l=round(w["volume"], 3), elapsed_s=round(elapsed))
            self.say(f"REGANDO  caudal {flow:5.1f} L/min | acumulado {w['volume']:7.2f} L | "
                     f"{elapsed:4.0f}/{w['duration']} s | operario {w['operator_id']}")
            if elapsed >= w["duration"]:
                self._end(w, "TIMEOUT")
                return

    def _end(self, w, reason):
        with self.lock:
            if self.watering is not w:
                return
            self.watering = None
        w["stop"].set()
        dur = time.time() - w["started"]
        self._tele("ENDED", req_id=w["req_id"], operator_id=w["operator_id"], reason=reason,
                   volume_l=round(w["volume"], 3), duration_s=round(dur, 1))
        self.say(f"Electroválvula CERRADA ({reason}). Total: {w['volume']:.2f} L en {dur:.0f} s")

    # ------------------------------------------------------------------ acciones locales (menú)
    def manual_start(self, seconds=None):
        if not (self.ws_id and self.link_ok):
            self.say("Sin identidad/enlace con el Monitor: no se puede solicitar riego")
            return
        self.say("Solicitando riego manual a CENTRAL ...")
        self._tele("START_REQ", operator_id="LOCAL", duration_s=seconds or self.default_duration)

    def manual_end(self):
        with self.lock:
            w = self.watering
        if w:
            self._end(w, "MANUAL")
        else:
            self.say("No hay riego en curso")

    def toggle_ko(self):
        with self.lock:
            self.ko = not self.ko
            w = self.watering
        if self.ko:
            self.say("*** FUGA SIMULADA *** (el Monitor recibirá KO y avisará a CENTRAL)")
            if w:
                self._end(w, "LEAK")
        else:
            self.say("Fuga reparada: el Engine vuelve a responder OK")
        self._send_status()

    def console_loop(self):
        print(HELP, flush=True)
        while self.running:
            try:
                line = input().strip().lower()
            except EOFError:
                return
            cmd, _, arg = line.partition(" ")
            if cmd == "s":
                self.manual_start(int(arg) if arg.strip().isdigit() else None)
            elif cmd == "e":
                self.manual_end()
            elif cmd == "k":
                self.toggle_ko()
            elif cmd == "q":
                self.running = False
                os._exit(0)
            else:
                print(HELP, flush=True)


def main():
    ap = argparse.ArgumentParser(prog="WM_WS_E", description="Engine de la estación de riego")
    ap.add_argument("--broker", default=env("KAFKA_BROKER", "localhost:9092"), help="IP:puerto del broker Kafka")
    ap.add_argument("--monitor", default=env("WM_MONITOR", "localhost:7001"), help="IP:puerto del Monitor de la WS")
    ap.add_argument("--base-flow", type=float, default=float(env("WM_BASE_FLOW", 12)), help="caudal medio L/min")
    ap.add_argument("--default-duration", type=int, default=int(env("WM_DEFAULT_DURATION", 30)))
    args = ap.parse_args()
    bus = get_bus(args.broker, "wm-ws-engine")
    eng = Engine(bus, parse_hostport(args.monitor), args.base_flow, args.default_duration,
                 ttl=float(env("WM_MSG_TTL", 60)))
    eng.start()
    threading.Thread(target=eng.console_loop, daemon=True).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
