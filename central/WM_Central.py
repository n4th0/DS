
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WM_Central - Sistema de monitorización y control de la red de riego.

* Servidor de sockets (STX/ETX/LRC) para los Monitores de las estaciones: autenticación,
  registro, latido y avería/reparación (fuga).
* Consumidor/productor Kafka: peticiones de los operarios (FO), telemetría y órdenes a las WS.
* Base de datos SQLite: estaciones, operarios, riegos y auditoría.
* Panel web de monitorización y control (Flask + waitress).

Uso:
    python WM_Central.py --socket-port 9000 --broker kafka.railway.internal:9092
(todos los argumentos tienen valor por defecto leído de variables de entorno)
"""
import argparse
import collections
import hmac
import os
import re
import secrets
import socket
import socketserver
import sys
import threading
import time
import traceback
import uuid

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
sys.path.insert(0, os.path.join(_HERE, ".."))

from wm_common import protocol as proto          # noqa: E402
from wm_common import topics as T                # noqa: E402
from wm_common.bus import get_bus                # noqa: E402
from wm_common.util import env, is_stale, say    # noqa: E402
from central_db import CentralDB                 # noqa: E402

AVAILABLE, WATERING, LEAK, OOS, DISCONNECTED = (
    "AVAILABLE", "WATERING", "LEAK", "OUT_OF_SERVICE", "DISCONNECTED")
STATE_ES = {AVAILABLE: "Disponible", WATERING: "Regando", LEAK: "Fuga",
            OOS: "Fuera de servicio", DISCONNECTED: "Desconectada"}
REASON_ES = {
    "TIMEOUT": "tiempo límite alcanzado",
    "MANUAL": "finalizado manualmente en la estación",
    "LEAK": "fuga detectada",
    "BLOCKED_BY_CENTRAL": "estación bloqueada por CENTRAL",
    "BLOCKED": "estación bloqueada",
    "DISCONNECTED": "estación desconectada de CENTRAL",
    "MONITOR_LOST": "la estación perdió su monitor",
    "STOPPED": "detenido por CENTRAL",
}

START_TIMEOUT = 8.0          # s que CENTRAL espera a que el Engine confirme el inicio
FINALIZE_FALLBACK = 4.0      # s que CENTRAL espera al ENDED del Engine antes de cerrar el riego por su cuenta
ENGINE_STATUS_MAX_AGE = 7.0  # s sin STATUS del Engine => no se autoriza el riego
ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,32}$")


class Station:
    def __init__(self, row):
        self.id = row["id"]
        self.location = row["location"]
        self.blocked = bool(row["blocked"])
        self.last_watering = row["last_watering"]
        self.connected = False
        self.leak = False
        self.token = None
        self.conn_id = None
        self.peer = None
        self.reserved = None           # {"req_id","operator_id","duration","timer"}
        self.active = None             # {"req_id","operator_id","started","duration","volume","flow"}
        self.flow = 0.0
        self.volume = 0.0
        self.engine_state = None
        self.last_engine_status = 0.0
        self.last_state = None
        self.last_cmd = {}
        self.bad_token_warned = 0.0


class Central:
    def __init__(self, db_path, bus, hb_timeout=10.0, default_duration=30, ttl=60.0,
                 allow_self_reg=True, operators=("FO-01", "FO-02", "FO-03", "FO-04")):
        self.db = CentralDB(db_path)
        self.bus = bus
        self.hb_timeout = hb_timeout
        self.default_duration = default_duration
        self.ttl = ttl
        self.allow_self_reg = allow_self_reg
        self.lock = threading.RLock()
        self.msg_lock = threading.Lock()
        self.messages = collections.deque(maxlen=300)
        self.seen_reqs = collections.OrderedDict()
        self.started_at = time.time()
        self.socket_port = None
        self.server = None
        self.stations = {}
        self.db.ensure_operators(list(operators))
        for row in self.db.list_ws():
            st = Station(row)
            self.stations[st.id] = st
            self.db.set_state(st.id, DISCONNECTED)
            st.last_state = DISCONNECTED
        for ev in self.db.recent_events(30):
            self.messages.append({"ts": ev["ts"], "level": ev["level"], "source": ev["source"], "msg": ev["message"]})
        self.log("INFO", "CENTRAL",
                 f"Arranque. {len(self.stations)} estaciones en BD; todas DESCONECTADAS hasta que su Monitor conecte.")

    # ------------------------------------------------------------------ utilidades
    def log(self, level, source, msg):
        ev = {"ts": time.time(), "level": level, "source": source, "msg": msg}
        with self.msg_lock:
            self.messages.append(ev)
        say(f"{level:<5} {source}", msg)
        try:
            self.db.add_event(ev["ts"], level, source, msg)
        except Exception:
            pass

    def state_of(self, st):
        if not st.connected:
            return DISCONNECTED
        if st.leak:
            return LEAK
        if st.blocked:
            return OOS
        if st.active:
            return WATERING
        return AVAILABLE

    def _publish_cmd(self, st, cmd, **kw):
        self.bus.publish(T.WS_COMMANDS, st.id, {"cmd": cmd, "ws_id": st.id, **kw})

    def notify(self, operator_id, ws_id, req_id, type_, message, **data):
        """Mensaje de CENTRAL hacia el operario (topic wm.fo.events, key = operator_id)."""
        self.bus.publish(T.FO_EVENTS, operator_id, {
            "operator_id": operator_id, "ws_id": ws_id, "req_id": req_id,
            "type": type_, "message": message, **data})
        if type_ != "TICK":
            self.log("INFO", f"→{operator_id}", f"[{type_}] {message}")

    def _publish_status(self, st):
        a = st.active
        self.bus.publish(T.WS_STATUS, st.id, {
            "ws_id": st.id, "location": st.location, "state": self.state_of(st),
            "flow_lpm": round(st.flow, 1) if a else 0.0,
            "volume_l": round(st.volume, 2) if a else 0.0,
            "operator_id": a["operator_id"] if a else None})

    def _state_changed(self, st):
        new = self.state_of(st)
        if new != st.last_state:
            old = st.last_state
            st.last_state = new
            self.db.set_state(st.id, new)
            level = "ERROR" if new == LEAK else "WARN" if new in (OOS, DISCONNECTED) else "INFO"
            self.log(level, st.id, f"Estado: {STATE_ES.get(old, '-')} → {STATE_ES[new]}")
        self._publish_status(st)

    def start(self, socket_port):
        outer = self

        class Handler(MonitorHandler):
            central = outer

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        self.server = Server(("0.0.0.0", socket_port), Handler)
        self.socket_port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.log("INFO", "CENTRAL", f"Servidor de sockets escuchando en el puerto {self.socket_port}")
        self.ready = self.bus.subscribe([T.FO_REQUESTS, T.WS_TELEMETRY], self._dispatch,
                                        group_id="wm-central", from_start=True)
        threading.Thread(target=self._broadcast_loop, daemon=True).start()

    def _broadcast_loop(self):
        while True:
            time.sleep(5)
            try:
                with self.lock:
                    for st in self.stations.values():
                        self._publish_status(st)
            except Exception:
                pass

    # ------------------------------------------------------------------ sockets (Monitor)
    def socket_message(self, ctx, msg):
        parts = msg.split("#")
        op = parts[0]
        if op == "REG":
            return self._sock_reg(ctx, parts)
        with self.lock:
            st = self._auth(parts)
            if st is None or st.conn_id != ctx["conn_id"]:
                return "KO#REAUTH"
            if op == "HB":
                return "HB_OK"
            if op == "LEAK":
                self._set_leak(st, True)
                return "OK"
            if op == "FIX":
                self._set_leak(st, False)
                return "OK"
        return "KO#UNKNOWN_OP"

    def _auth(self, parts):
        if len(parts) < 3:
            return None
        st = self.stations.get(parts[1])
        if st and st.connected and st.token and hmac.compare_digest(st.token, parts[2]):
            return st
        return None

    def _sock_reg(self, ctx, parts):
        if len(parts) != 4:
            return "REG_KO#BAD_FORMAT"
        _, ws_id, location, secret = parts
        if not ID_RE.match(ws_id) or not secret:
            return "REG_KO#BAD_FORMAT"
        location = location.strip()[:64] or "Sin ubicación"
        with self.lock:
            row = self.db.get_ws(ws_id)
            if row is None:
                if not self.allow_self_reg:
                    self.log("WARN", "AUTH", f"Alta rechazada para {ws_id} desde {ctx['peer']} (no autorizada)")
                    return "REG_KO#UNKNOWN_WS"
                self.db.create_ws(ws_id, location, secret)
                st = Station(self.db.get_ws(ws_id))
                self.stations[ws_id] = st
                self.log("INFO", "AUTH", f"ALTA de nueva estación {ws_id} en '{location}' (desde {ctx['peer']})")
            else:
                if not self.db.verify_secret(row, secret):
                    self.log("WARN", "AUTH", f"Autenticación FALLIDA de {ws_id} desde {ctx['peer']}")
                    return "REG_KO#BAD_CREDENTIALS"
                st = self.stations[ws_id]
                if location != st.location and location != "Sin ubicación":
                    st.location = location
                    self.db.update_location(ws_id, location)
            if st.connected and st.conn_id != ctx["conn_id"]:
                self.log("WARN", "AUTH", f"{ws_id}: registro duplicado rechazado (ya hay un Monitor conectado)")
                return "REG_KO#ALREADY_CONNECTED"
            st.connected = True
            st.conn_id = ctx["conn_id"]
            st.peer = ctx["peer"]
            st.token = secrets.token_hex(8)
            st.leak = False
            st.engine_state = None
            st.last_engine_status = 0.0
            ctx["ws_id"] = ws_id
            self.log("INFO", "AUTH", f"{ws_id} AUTENTICADA y conectada ({ctx['peer']})")
            self._state_changed(st)
            return f"REG_OK#{st.token}"

    def on_disconnect(self, ctx, why=""):
        ws_id = ctx.get("ws_id")
        if not ws_id:
            return
        with self.lock:
            st = self.stations.get(ws_id)
            if not st or st.conn_id != ctx["conn_id"]:
                return
            st.connected = False
            st.token = None
            st.conn_id = None
            self.log("WARN", ws_id, f"Monitor desconectado ({why})")
            self._stop_watering(st, "DISCONNECTED", immediate=True)
            self._state_changed(st)

    # ------------------------------------------------------------------ Kafka
    def _dispatch(self, topic, key, m):
        try:
            if is_stale(m, self.ttl):
                return
            if topic == T.FO_REQUESTS:
                self.on_fo_request(m)
            elif topic == T.WS_TELEMETRY:
                self.on_telemetry(m)
        except Exception:
            self.log("ERROR", "CENTRAL", f"Error procesando mensaje de {topic}: {traceback.format_exc()}")

    def on_fo_request(self, m):
        req_id = str(m.get("req_id") or uuid.uuid4().hex[:8])
        with self.lock:
            if req_id in self.seen_reqs:
                return
            self.seen_reqs[req_id] = time.time()
            while len(self.seen_reqs) > 2000:
                self.seen_reqs.popitem(last=False)
        self.handle_activation(str(m.get("operator_id") or "?"), str(m.get("ws_id") or "?"),
                               m.get("duration_s"), origin="FO", req_id=req_id)

    # ------------------------------------------------------------------ lógica de activación
    def handle_activation(self, operator_id, ws_id, duration, origin="FO", req_id=None):
        """Valida y, si procede, ordena el inicio del riego. -> (ok, mensaje)"""
        req_id = req_id or uuid.uuid4().hex[:8]
        try:
            duration = int(duration)
        except (TypeError, ValueError):
            duration = self.default_duration
        duration = max(1, min(3600, duration))

        def info(msg):
            self.notify(operator_id, ws_id, req_id, "INFO", msg)

        def deny(msg):
            self.notify(operator_id, ws_id, req_id, "DENIED", msg)
            return False, msg

        info(f"Solicitud {req_id} recibida en CENTRAL: {operator_id} → {ws_id} ({duration} s)")
        if origin == "FO":
            if not self.db.operator_active(operator_id):
                return deny(f"Operario '{operator_id}' no registrado o inactivo en CENTRAL")
            info(f"Operario {operator_id} validado")
        with self.lock:
            st = self.stations.get(ws_id)
            if st is None:
                return deny(f"La estación {ws_id} no existe")
            info(f"Comprobando disponibilidad de {ws_id} ...")
            state = self.state_of(st)
            if state != AVAILABLE:
                return deny(f"Estación {ws_id} NO disponible (estado: {STATE_ES[state]})")
            if st.reserved:
                return deny(f"Estación {ws_id} ocupada: hay otra activación en curso")
            if time.time() - st.last_engine_status > ENGINE_STATUS_MAX_AGE:
                return deny(f"El Engine de {ws_id} no responde")
            timer = threading.Timer(START_TIMEOUT, self._reservation_timeout, (ws_id, req_id))
            timer.daemon = True
            st.reserved = {"req_id": req_id, "operator_id": operator_id, "duration": duration, "timer": timer}
            timer.start()
        info(f"{ws_id} disponible. Solicitando autorización/arranque a la estación ...")
        self._publish_cmd(st, "START", req_id=req_id, operator_id=operator_id, duration_s=duration)
        return True, f"Orden de riego enviada a {ws_id} (req {req_id})"

    def _release(self, st):
        if st.reserved:
            st.reserved["timer"].cancel()
            st.reserved = None

    def _reservation_timeout(self, ws_id, req_id):
        with self.lock:
            st = self.stations.get(ws_id)
            if not st or not st.reserved or st.reserved["req_id"] != req_id:
                return
            op = st.reserved["operator_id"]
            st.reserved = None
        self.notify(op, ws_id, req_id, "DENIED", f"La estación {ws_id} no confirmó el inicio del riego (timeout)")

    # ------------------------------------------------------------------ telemetría de las WS
    def on_telemetry(self, m):
        st = self.stations.get(m.get("ws_id"))
        if st is None:
            return
        with self.lock:
            tok = m.get("token")
            if not st.connected or not st.token or not isinstance(tok, str) or not hmac.compare_digest(st.token, tok):
                if time.time() - st.bad_token_warned > 30:
                    st.bad_token_warned = time.time()
                    self.log("WARN", "AUTH", f"Telemetría rechazada de {st.id}: token inválido o Monitor no autenticado")
                return
            t = m.get("type")
            if t == "STATUS":
                st.engine_state = m.get("engine_state")
                st.last_engine_status = time.time()
                self._reconcile(st)
            elif t == "START_REQ":
                self.handle_activation(str(m.get("operator_id") or "LOCAL"), st.id, m.get("duration_s"),
                                       origin="LOCAL")
            elif t == "STARTED":
                self._on_started(st, m)
            elif t == "START_DENIED":
                res = st.reserved
                if res and res["req_id"] == m.get("req_id"):
                    self._release(st)
                    self.notify(res["operator_id"], st.id, res["req_id"], "DENIED",
                                f"La estación {st.id} rechazó el riego: {REASON_ES.get(m.get('reason'), m.get('reason'))}")
            elif t == "TICK":
                self._on_tick(st, m)
            elif t == "ENDED":
                a = st.active
                if a and a["req_id"] == m.get("req_id"):
                    self._finalize(st, m.get("reason", "?"), m.get("volume_l"), m.get("duration_s"))

    def _on_started(self, st, m):
        res = st.reserved
        if res and res["req_id"] == m.get("req_id"):
            self._release(st)
            op, req, dur = res["operator_id"], res["req_id"], res["duration"]
        elif self._adopt(st, m):
            return
        else:
            self._publish_cmd(st, "STOP", reason="STOPPED")
            return
        st.active = {"req_id": req, "operator_id": op, "started": time.time(), "duration": dur,
                     "volume": 0.0, "flow": 0.0}
        st.flow = st.volume = 0.0
        self.db.start_watering(req, st.id, op, st.active["started"])
        self.notify(op, st.id, req, "ACCEPTED",
                    f"Riego AUTORIZADO en {st.id} ({st.location}). Válvula abierta, duración máx. {dur} s")
        self._state_changed(st)

    def _adopt(self, st, m):
        """Un Engine que ya regaba (p. ej. tras reiniciar CENTRAL) se re-incorpora al estado."""
        if st.active or st.leak or st.blocked or not m.get("req_id"):
            return False
        op = str(m.get("operator_id") or "?")
        st.active = {"req_id": m["req_id"], "operator_id": op, "started": time.time(),
                     "duration": int(m.get("duration_s") or 0), "volume": 0.0, "flow": 0.0}
        self.db.start_watering(m["req_id"], st.id, op, st.active["started"])
        self.log("WARN", st.id, f"Riego en curso ({m['req_id']}) re-adoptado tras reinicio/reconexión")
        self._state_changed(st)
        return True

    def _on_tick(self, st, m):
        a = st.active
        if not a or a["req_id"] != m.get("req_id"):
            if not self._adopt(st, m):
                self._publish_cmd(st, "STOP", reason="STOPPED")
            return
        a["flow"] = st.flow = float(m.get("flow_lpm", 0))
        a["volume"] = st.volume = float(m.get("volume_l", 0))
        self.notify(a["operator_id"], st.id, a["req_id"], "TICK",
                    f"{st.id} regando", flow_lpm=round(st.flow, 1), volume_l=round(st.volume, 2),
                    elapsed_s=m.get("elapsed_s"))

    def _reconcile(self, st):
        """Mantiene el Engine alineado con el bloqueo decidido por CENTRAL (persistente en BD)."""
        want = "BLOCK" if st.blocked else ("UNBLOCK" if st.engine_state == "BLOCKED" else None)
        if want == "BLOCK" and st.engine_state == "BLOCKED":
            want = None
        if want and time.time() - st.last_cmd.get(want, 0) > 5:
            st.last_cmd[want] = time.time()
            self._publish_cmd(st, want, reason="BLOCKED_BY_CENTRAL")

    # ------------------------------------------------------------------ fin de riego / fugas
    def _set_leak(self, st, flag):
        if st.leak == flag:
            return
        st.leak = flag
        if flag:
            self.log("ERROR", st.id, "FUGA/AVERÍA comunicada por el Monitor: no se permite riego")
            self._stop_watering(st, "LEAK")
        else:
            self.log("INFO", st.id, "Avería resuelta: la estación vuelve a estar disponible")
        self._state_changed(st)

    def _stop_watering(self, st, reason, immediate=False):
        why = REASON_ES.get(reason, reason)
        if st.reserved:
            res = st.reserved
            self._release(st)
            self.notify(res["operator_id"], st.id, res["req_id"], "DENIED", f"Activación cancelada: {why}")
        a = st.active
        if a:
            self._publish_cmd(st, "STOP", reason=reason)
            self.notify(a["operator_id"], st.id, a["req_id"], "ALERT", f"Riego INTERRUMPIDO en {st.id}: {why}")
            if immediate:
                self._finalize(st, reason)
            else:
                t = threading.Timer(FINALIZE_FALLBACK, self._finalize_fallback, (st.id, a["req_id"], reason))
                t.daemon = True
                t.start()

    def _finalize_fallback(self, ws_id, req_id, reason):
        with self.lock:
            st = self.stations.get(ws_id)
            if st and st.active and st.active["req_id"] == req_id:
                self._finalize(st, reason)

    def _finalize(self, st, reason, volume=None, duration=None):
        a = st.active
        if not a:
            return
        vol = a["volume"] if volume is None else float(volume)
        dur = (time.time() - a["started"]) if duration is None else float(duration)
        st.active = None
        st.flow = st.volume = 0.0
        st.last_watering = time.time()
        self.db.end_watering(a["req_id"], st.last_watering, dur, vol, reason)
        self.db.set_last_watering(st.id, st.last_watering)
        why = REASON_ES.get(reason, reason)
        self.notify(a["operator_id"], st.id, a["req_id"], "SUMMARY",
                    f"Riego finalizado en {st.id}: {vol:.2f} L en {dur:.0f} s ({why})",
                    volume_l=round(vol, 2), duration_s=round(dur, 1), reason=reason)
        self._state_changed(st)

    # ------------------------------------------------------------------ órdenes desde el panel
    def command(self, action, target, duration=None):
        results = []
        with self.lock:
            ids = sorted(self.stations) if target == "ALL" else [target]
        if not ids:
            return [f"No hay estaciones registradas"]
        for ws_id in ids:
            st = self.stations.get(ws_id)
            if st is None:
                results.append(f"{ws_id}: no existe")
                continue
            if action == "start":
                ok, msg = self.handle_activation("CENTRAL", ws_id, duration or self.default_duration, origin="CENTRAL")
                results.append(f"{ws_id}: {msg}")
            elif action == "block":
                with self.lock:
                    st.blocked = True
                    self.db.set_blocked(ws_id, True)
                    self.log("WARN", ws_id, "CENTRAL ordena BLOQUEAR la estación (Fuera de Servicio)")
                    self._stop_watering(st, "BLOCKED_BY_CENTRAL")
                    self._publish_cmd(st, "BLOCK", reason="BLOCKED_BY_CENTRAL")
                    self._state_changed(st)
                results.append(f"{ws_id}: bloqueada")
            elif action == "activate":
                with self.lock:
                    st.blocked = False
                    self.db.set_blocked(ws_id, False)
                    self.log("INFO", ws_id, "CENTRAL ordena ACTIVAR la estación")
                    self._publish_cmd(st, "UNBLOCK", reason="ACTIVATED")
                    self._state_changed(st)
                results.append(f"{ws_id}: activada")
            else:
                results.append(f"acción desconocida: {action}")
        return results

    # ------------------------------------------------------------------ panel
    def snapshot(self):
        with self.lock:
            rows, counts = [], {s: 0 for s in STATE_ES}
            for st in sorted(self.stations.values(), key=lambda s: s.id):
                state = self.state_of(st)
                counts[state] += 1
                a = st.active
                rows.append({"id": st.id, "location": st.location, "state": state,
                             "flow_lpm": round(st.flow, 1) if a else None,
                             "volume_l": round(st.volume, 2) if a else None,
                             "operator_id": a["operator_id"] if a else None,
                             "elapsed_s": round(time.time() - a["started"]) if a else None,
                             "last_watering": st.last_watering})
            connected = sum(1 for s in self.stations.values() if s.connected)
        with self.msg_lock:
            msgs = list(self.messages)[-120:]
        return {"stations": rows, "summary": counts, "messages": msgs,
                "system": {"uptime_s": round(time.time() - self.started_at), "bus": self.bus.describe(),
                           "socket_port": self.socket_port, "monitors_connected": connected,
                           "stations_registered": len(rows), "now": time.time()}}


class MonitorHandler(socketserver.BaseRequestHandler):
    """Una conexión por Monitor (sesión persistente: sirve también para detectar desconexiones)."""
    central = None

    def handle(self):
        c = self.central
        peer = f"{self.client_address[0]}:{self.client_address[1]}"
        try:
            self.request.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except OSError:
            pass
        ch = proto.Channel(self.request, timeout=c.hb_timeout)
        ctx = {"conn_id": uuid.uuid4().hex[:8], "ws_id": None, "peer": peer}
        why = "cierre"
        try:
            proto.accept_handshake(ch)
            while True:
                msg = proto.recv_request(ch)
                proto.send_answer(ch, c.socket_message(ctx, msg))
        except proto.EndOfTransmission:
            why = "EOT"
        except proto.ChannelTimeout:
            why = f"sin latidos durante {c.hb_timeout:.0f} s"
        except (proto.ProtocolError, OSError) as e:
            why = str(e)
        except Exception:
            why = "error interno"
            c.log("ERROR", "CENTRAL", traceback.format_exc())
        finally:
            c.on_disconnect(ctx, why)
            ch.close()


# ---------------------------------------------------------------------- HTTP
def create_app(central, user, password):
    from flask import Flask, Response, jsonify, request, send_from_directory
    app = Flask(__name__, static_folder=None)

    @app.before_request
    def _auth():
        if request.path == "/healthz" or not password:
            return None
        a = request.authorization
        if a and hmac.compare_digest(a.username or "", user) and hmac.compare_digest(a.password or "", password):
            return None
        return Response("Autenticación requerida", 401, {"WWW-Authenticate": 'Basic realm="WaterManagement"'})

    @app.get("/healthz")
    def healthz():
        return "ok"

    @app.get("/")
    def index():
        return send_from_directory(os.path.join(_HERE, "static"), "index.html")

    @app.get("/api/state")
    def state():
        return jsonify(central.snapshot())

    @app.post("/api/command")
    def command():
        body = request.get_json(silent=True) or {}
        action, target = body.get("action"), str(body.get("target") or "")
        if action not in ("start", "block", "activate") or not target:
            return jsonify({"ok": False, "error": "petición inválida"}), 400
        return jsonify({"ok": True, "results": central.command(action, target, body.get("duration"))})

    return app


def main():
    ap = argparse.ArgumentParser(prog="WM_Central", description="Central de la red de riego")
    ap.add_argument("--socket-port", type=int, default=int(env("WM_SOCKET_PORT", 9000)),
                    help="puerto del servidor de sockets para los Monitores (WM_WS_M)")
    ap.add_argument("--broker", default=env("KAFKA_BROKER", "localhost:9092"),
                    help="IP:puerto del Broker/Bootstrap-server de Kafka")
    ap.add_argument("--http-port", type=int, default=int(env("PORT", 8080)), help="puerto del panel web")
    ap.add_argument("--db", default=env("WM_DB_PATH", "central.db"), help="ruta del fichero SQLite")
    args = ap.parse_args()

    bus = get_bus(args.broker, "wm-central")
    operators = [o.strip() for o in env("WM_OPERATORS", "FO-01,FO-02,FO-03,FO-04").split(",") if o.strip()]
    central = Central(args.db, bus,
                      hb_timeout=float(env("WM_HB_TIMEOUT", 10)),
                      default_duration=int(env("WM_DEFAULT_DURATION", 30)),
                      ttl=float(env("WM_MSG_TTL", 60)),
                      allow_self_reg=env("WM_ALLOW_SELF_REGISTRATION", "true").lower() == "true",
                      operators=operators)
    central.start(args.socket_port)

    user, password = env("PANEL_USER", "admin"), env("PANEL_PASSWORD")
    if not password:
        central.log("WARN", "CENTRAL", "PANEL_PASSWORD no definida: el panel web queda SIN autenticación")
    app = create_app(central, user, password)
    central.log("INFO", "CENTRAL", f"Panel web en el puerto {args.http_port}")
    from waitress import serve
    serve(app, host="0.0.0.0", port=args.http_port, threads=8, _quiet=True)


if __name__ == "__main__":
    main()
# trigger
