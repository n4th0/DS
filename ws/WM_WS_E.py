#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WM_WS_E - Engine de la Watering Station (caudalímetro + electroválvula simulados).

* Conecta por sockets con su Monitor (WM_WS_M): recibe su ID y token de sesión (validados por CENTRAL)
  y responde cada segundo a las comprobaciones de salud (OK/KO). El Monitor le indica además si
  CENTRAL está disponible (UP/DOWN) y el Engine lo muestra por consola.
* Recibe órdenes de CENTRAL por Kafka (START/STOP/BLOCK/UNBLOCK) y envía telemetría
  (STATUS cada 2 s, TICK cada segundo regando, STARTED, ENDED).
* Si el Monitor se cae, el Engine NO corta el riego: sigue cumpliendo las órdenes recibidas, guarda
  lo que va ocurriendo (STARTED/ENDED/START_DENIED) y lo informa a CENTRAL cuando el Monitor vuelve.
* Menú por teclado (stdin):  s [seg] = regar manualmente | e = finalizar riego |
  k = simular/quitar FUGA (KO al monitor) | c = estado de CENTRAL | h = ayuda | q = salir.

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

OFFLINE_TYPES = ("STARTED", "ENDED", "START_DENIED")   # eventos que se guardan mientras no hay Monitor

HELP = ("Comandos:  s [segundos] = regar manualmente | e = finalizar riego | "
        "k = simular/quitar FUGA | c = estado de CENTRAL | h = ayuda | q = salir")


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
        self.pending = []                # eventos ocurridos sin enlace con el Monitor (se informan al volver)
        self.central_ok = None           # None = desconocido, True = disponible, False = caído
        self.central_since = time.time()

    def say(self, msg):
        say(f"ENGINE {self.ws_id or '?'}", msg)

    # ------------------------------------------------------------------ estado de CENTRAL
    def set_central(self, up, why=""):
        """Actualiza el estado de CENTRAL y lo notifica solo cuando cambia."""
        with self.lock:
            if self.central_ok is up:
                return
            self.central_ok = up
            self.central_since = time.time()
            w = self.watering
        if up:
            self.say(f"*** CENTRAL DISPONIBLE *** {why}".rstrip())
        elif up is False:
            self.say(f"*** CENTRAL CAÍDO *** {why}".rstrip())
            if w:
                self.say("Hay un riego en curso: continúa hasta su fin, pero CENTRAL no recibe la telemetría")
        else:
            self.say("Estado de CENTRAL desconocido (sin enlace con el Monitor)")
        self._send_status()

    def _warn_central(self):
        """Recordatorio periódico mientras no haya conexión confirmada con CENTRAL."""
        secs = time.time() - self.central_since
        if self.central_ok is False:
            self.say(f"!!! SIN CONEXIÓN CON CENTRAL desde hace {secs:.0f} s !!!")
        else:
            self.say(f"!!! SIN CONEXIÓN CON CENTRAL (sin enlace con el Monitor, {secs:.0f} s) !!!")

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
                if "NOT_REGISTERED" not in str(e):
                    self.say(f"Enlace con el Monitor perdido: {e}")
            except Exception:
                self.say("Error inesperado en el enlace:\n" + traceback.format_exc())
            was_linked = self.link_ok
            self.link_ok = False
            if was_linked:
                self.set_central(None)
                with self.lock:
                    w = self.watering
                if w:
                    self.say("Monitor perdido: el riego en curso CONTINÚA según la orden recibida; "
                             "se informará de todo cuando el Monitor vuelva")
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
                if "NOT_REGISTERED" in ans:
                    self.set_central(False, "(el Monitor no está autenticado en CENTRAL)")
                raise proto.ProtocolError(f"el Monitor no está listo ({ans})")
            self.ws_id, self.token = parts[1], parts[2]
            self.say("Identidad recibida del Monitor (validada por CENTRAL)")
            self._ensure_subscribed()
            self.link_ok = True
            self._flush_pending()
            self._send_status()
            while self.running:
                msg = proto.recv_request(ch)       # el Monitor pregunta; timeout => Monitor perdido
                p = msg.split("#")
                if p[0] == "HEALTH":
                    if len(p) > 2 and p[2] != "-":
                        self.token = p[2]          # CENTRAL pudo haber renovado el token
                    if len(p) > 3 and p[3] in ("UP", "DOWN"):
                        self.set_central(p[3] == "UP", "(informado por el Monitor)")
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
        if not self.link_ok:
            # Sin Monitor no hay token de sesión válido: lo importante se guarda y se informa al reconectar
            if type_ in OFFLINE_TYPES:
                with self.lock:
                    self.pending.append({"type": type_, "at": time.time(), **kw})
            return
        self.bus.publish(T.WS_TELEMETRY, self.ws_id,
                         {"type": type_, "ws_id": self.ws_id, "token": self.token, **kw})

    def _flush_pending(self):
        """Informa a CENTRAL (con el token nuevo que acaba de dar el Monitor) de lo ocurrido sin enlace."""
        with self.lock:
            pend, self.pending = self.pending, []
        if not pend:
            return
        self.say(f"Informando a CENTRAL de {len(pend)} evento(s) ocurridos sin enlace con el Monitor")
        for ev in pend:
            ev = dict(ev)
            type_, at = ev.pop("type"), ev.pop("at")
            self._tele(type_, offline=True, at=at, **ev)

    def _send_status(self):
        self._tele("STATUS", engine_state=self.state, central_ok=self.central_ok)

    def _status_loop(self):
        last_warn = 0.0
        while self.running:
            if self.link_ok and self.subscribed:
                self._send_status()
            if self.central_ok is not True and time.time() - last_warn >= 5:
                last_warn = time.time()
                self._warn_central()
            time.sleep(2)

    # ------------------------------------------------------------------ órdenes de CENTRAL
    def _on_command(self, topic, key, m):
        if m.get("ws_id") != self.ws_id or is_stale(m, self.ttl):
            return
        self.set_central(True, "(orden recibida por Kafka)")
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
        verb = "REANUDADO tras fuga" if m.get("resume") else "solicitado"
        self.say(f"Electroválvula ABIERTA: riego {req} {verb} por {op}, máx. {dur} s")
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
        if self.central_ok is False:
            self.say("CENTRAL está caído: la petición puede no ser atendida")
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

    def show_central(self):
        est = {True: "DISPONIBLE", False: "CAÍDO", None: "DESCONOCIDO"}[self.central_ok]
        self.say(f"CENTRAL {est} desde hace {time.time() - self.central_since:.0f} s")

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
            elif cmd == "c":
                self.show_central()
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