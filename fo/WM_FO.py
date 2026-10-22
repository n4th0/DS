#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WM_FO - Aplicación del operario de campo (Field Operator).

* Solicita riegos a CENTRAL por Kafka y muestra en pantalla cada paso del proceso
  (validación, autorización/denegación, telemetría cada segundo y resumen final).
* Solo recibe los mensajes dirigidos a su operario, pero ve el estado de TODAS las estaciones.
* Modo fichero: lee los servicios a solicitar; pide el primero y, al concluir (éxito o fracaso),
  espera 4 s y pide el siguiente.

Formato del fichero (una línea por servicio; '#' al inicio = comentario):
    WS_ID,[duracion_en_segundos]
    WS-01,20
    WS-02

Uso:
    python WM_FO.py --broker IP:PUERTO --operator-id FO-01 [--file servicios.csv] [--no-menu]
"""
import argparse
import os
import sys
import threading
import time
import uuid

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, ".."))

from wm_common import topics as T
from wm_common.bus import get_bus
from wm_common.util import env, is_stale, say

STATE_ES = {"AVAILABLE": "Disponible", "WATERING": "Regando", "LEAK": "Fuga",
            "OUT_OF_SERVICE": "Fuera de servicio", "DISCONNECTED": "Desconectada"}


def parse_services_file(path, default_duration):
    services = []
    with open(path, encoding="utf-8") as f:
        for n, raw in enumerate(f, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            ws, _, dur = (p.strip() for p in line.partition(","))
            if not ws:
                raise ValueError(f"{path}:{n}: línea inválida '{raw.strip()}'")
            services.append((ws, int(dur) if dur.isdigit() else default_duration))
    return services


class FieldOperator:
    def __init__(self, bus, operator_id, default_duration=30, ttl=60.0, grace=1.0):
        self.bus = bus
        self.op = operator_id
        self.default_duration = default_duration
        self.ttl = ttl
        self.grace = grace
        self.stations = {}
        self.pending = {}
        self.lock = threading.Lock()

    def say(self, msg):
        say(f"FO {self.op}", msg)

    def start(self):
        uid = uuid.uuid4().hex[:6]
        r1 = self.bus.subscribe([T.WS_STATUS], self._on_status, group_id=f"fo-status-{self.op}-{uid}")
        r2 = self.bus.subscribe([T.FO_EVENTS], self._on_event, group_id=f"fo-events-{self.op}-{uid}")
        r1.wait(30)
        r2.wait(30)
        time.sleep(self.grace)

    # recepción
    def _on_status(self, topic, key, m):
        with self.lock:
            self.stations[m.get("ws_id")] = m

    def _on_event(self, topic, key, m):
        if m.get("operator_id") != self.op or is_stale(m, self.ttl):
            return
        t, ws, msg = m.get("type"), m.get("ws_id"), m.get("message", "")
        if t == "INFO":
            self.say(f"CENTRAL: {msg}")
        elif t == "ACCEPTED":
            self.say(f"✔ AUTORIZADO  {msg}")
        elif t == "TICK":
            self.say(f"{ws} REGANDO  caudal {m.get('flow_lpm')} L/min | acumulado {m.get('volume_l')} L | "
                     f"{m.get('elapsed_s')} s")
        elif t == "ALERT":
            self.say(f"⚠ ALERTA  {msg}")
        elif t == "DENIED":
            self.say(f"✘ DENEGADO  {msg}")
        elif t == "SUMMARY":
            self.say(f"RESUMEN  {msg}")
        if t in ("DENIED", "SUMMARY"):
            with self.lock:
                entry = self.pending.get(m.get("req_id"))
            if entry:
                entry["result"] = t
                entry["event"].set()

    # acciones
    def request_watering(self, ws_id, duration=None, timeout=None):
        """Pide un riego y espera a su conclusión. -> 'SUMMARY' | 'DENIED' | 'TIMEOUT'"""
        duration = duration or self.default_duration
        req_id = uuid.uuid4().hex[:8]
        entry = {"event": threading.Event(), "result": None}
        with self.lock:
            self.pending[req_id] = entry
        self.say(f"Solicitando riego {req_id}: {ws_id} durante {duration} s ...")
        self.bus.publish(T.FO_REQUESTS, self.op,
                         {"req_id": req_id, "operator_id": self.op, "ws_id": ws_id, "duration_s": duration})
        if not entry["event"].wait(timeout or duration + 30):
            self.say("Sin conclusión de CENTRAL (timeout)")
            entry["result"] = "TIMEOUT"
        with self.lock:
            self.pending.pop(req_id, None)
        return entry["result"]

    def run_file(self, path, pause=4.0):
        services = parse_services_file(path, self.default_duration)
        self.say(f"Fichero {path}: {len(services)} servicios")
        results = []
        for i, (ws, dur) in enumerate(services):
            results.append(self.request_watering(ws, dur))
            if i < len(services) - 1:
                self.say(f"Esperando {pause:.0f} s antes del siguiente servicio ...")
                time.sleep(pause)
        self.say("Fichero de servicios completado")
        return results

    def list_stations(self):
        with self.lock:
            sts = sorted(self.stations.values(), key=lambda s: s["ws_id"])
        if not sts:
            print("  (aún no se ha recibido el estado de ninguna estación)")
        for s in sts:
            extra = f" | {s['flow_lpm']} L/min, {s['volume_l']} L ({s['operator_id']})" if s["state"] == "WATERING" else ""
            mark = "●" if s["state"] == "AVAILABLE" else "○"
            print(f"  {mark} {s['ws_id']:<8} {s['location']:<22} {STATE_ES.get(s['state'], s['state'])}{extra}")

    def menu(self):
        while True:
            print("\n1) Ver estaciones   2) Solicitar riego   3) Ejecutar fichero   4) Salir", flush=True)
            try:
                opt = input("> ").strip()
                if opt == "1":
                    self.list_stations()
                elif opt == "2":
                    ws = input("ID de la estación: ").strip()
                    d = input(f"Duración en segundos [{self.default_duration}]: ").strip()
                    if ws:
                        self.request_watering(ws, int(d) if d.isdigit() else None)
                elif opt == "3":
                    p = input("Ruta del fichero: ").strip()
                    if p:
                        self.run_file(p)
                elif opt == "4":
                    return
            except EOFError:
                return
            except (OSError, ValueError) as e:
                self.say(f"Error: {e}")


def main():
    ap = argparse.ArgumentParser(prog="WM_FO", description="Aplicación del operario de campo")
    ap.add_argument("--broker", default=env("KAFKA_BROKER", "localhost:9092"), help="IP:puerto del broker Kafka")
    ap.add_argument("--operator-id", default=env("WM_OPERATOR_ID"), required=env("WM_OPERATOR_ID") is None,
                    help="ID del operario dado de alta en CENTRAL")
    ap.add_argument("--file", default=env("WM_SERVICES_FILE"), help="fichero con los servicios a solicitar")
    ap.add_argument("--duration", type=int, default=int(env("WM_DEFAULT_DURATION", 30)))
    ap.add_argument("--no-menu", action="store_true", default=env("WM_NO_MENU", "false").lower() == "true",
                    help="salir al terminar el fichero (sin menú interactivo)")
    args = ap.parse_args()
    bus = get_bus(args.broker, f"wm-fo-{args.operator_id}")
    fo = FieldOperator(bus, args.operator_id, args.duration, ttl=float(env("WM_MSG_TTL", 60)))
    fo.start()
    if args.file:
        fo.run_file(args.file)
    if args.no_menu:
        return
    if not sys.stdin.isatty():
        fo.say("Sin terminal interactiva (usa 'docker run -it' o --file ... --no-menu). Quedo escuchando.")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            return
    fo.menu()


if __name__ == "__main__":
    main()
