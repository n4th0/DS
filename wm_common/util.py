"""Utilidades comunes"""
import os
import threading
import time

_print_lock = threading.Lock()


def env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def parse_hostport(value):
    host, sep, port = value.strip().rpartition(":")
    if not sep or not host or not port.isdigit():
        raise ValueError(f"Se esperaba host:puerto y se recibió '{value}'")
    return host, int(port)


def hhmmss(ts=None):
    return time.strftime("%H:%M:%S", time.localtime(ts or time.time()))


def say(tag, msg):
    """Salida por pantalla con marca de tiempo (thread-safe)."""
    with _print_lock:
        print(f"[{hhmmss()}] [{tag}] {msg}", flush=True)


def is_stale(message, ttl):
    """True si el mensaje de Kafka es mas viejo que `ttl` segundos"""
    ts = message.get("ts")
    return isinstance(ts, (int, float)) and (time.time() - ts) > ttl
