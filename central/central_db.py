
"""Capa de persistencia de CENTRAL (SQLite)."""
import hashlib
import hmac
import os
import secrets
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS ws(
    id TEXT PRIMARY KEY,
    location TEXT NOT NULL,
    secret_hash TEXT NOT NULL,
    salt TEXT NOT NULL,
    blocked INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'DISCONNECTED',
    last_watering REAL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS operators(
    id TEXT PRIMARY KEY,
    name TEXT,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS waterings(
    req_id TEXT PRIMARY KEY,
    ws_id TEXT NOT NULL,
    operator_id TEXT NOT NULL,
    started_at REAL NOT NULL,
    ended_at REAL,
    duration_s REAL,
    volume_l REAL,
    end_reason TEXT
);
CREATE TABLE IF NOT EXISTS events(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    level TEXT,
    source TEXT,
    message TEXT
);
"""


def hash_secret(secret: str, salt_hex: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", secret.encode("utf-8"), bytes.fromhex(salt_hex), 50_000).hex()


class CentralDB:
    def __init__(self, path):
        folder = os.path.dirname(os.path.abspath(path))
        os.makedirs(folder, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.executescript(SCHEMA)

    def _run(self, sql, args=()):
        with self.lock:
            return self.conn.execute(sql, args).fetchall()

    # ---- operarios
    def ensure_operators(self, ids):
        for op in ids:
            self._run("INSERT OR IGNORE INTO operators(id, name, active) VALUES(?,?,1)", (op, op))

    def operator_active(self, op_id):
        rows = self._run("SELECT active FROM operators WHERE id=?", (op_id,))
        return bool(rows and rows[0]["active"])

    def list_operators(self):
        return [dict(r) for r in self._run("SELECT id, name, active FROM operators ORDER BY id")]

    # ---- estaciones
    def list_ws(self):
        return self._run("SELECT * FROM ws ORDER BY id")

    def get_ws(self, ws_id):
        rows = self._run("SELECT * FROM ws WHERE id=?", (ws_id,))
        return rows[0] if rows else None

    def create_ws(self, ws_id, location, secret):
        salt = secrets.token_hex(8)
        self._run("INSERT INTO ws(id, location, secret_hash, salt, created_at) VALUES(?,?,?,?,?)",
                  (ws_id, location, hash_secret(secret, salt), salt, time.time()))

    @staticmethod
    def verify_secret(row, secret):
        return hmac.compare_digest(hash_secret(secret, row["salt"]), row["secret_hash"])

    def update_location(self, ws_id, location):
        self._run("UPDATE ws SET location=? WHERE id=?", (location, ws_id))

    def set_blocked(self, ws_id, blocked):
        self._run("UPDATE ws SET blocked=? WHERE id=?", (1 if blocked else 0, ws_id))

    def set_state(self, ws_id, state):
        self._run("UPDATE ws SET state=? WHERE id=?", (state, ws_id))

    def set_last_watering(self, ws_id, ts):
        self._run("UPDATE ws SET last_watering=? WHERE id=?", (ts, ws_id))

    # ---- riegos
    def start_watering(self, req_id, ws_id, operator_id, started_at):
        self._run("INSERT OR REPLACE INTO waterings(req_id, ws_id, operator_id, started_at) VALUES(?,?,?,?)",
                  (req_id, ws_id, operator_id, started_at))

    def end_watering(self, req_id, ended_at, duration_s, volume_l, reason):
        self._run("UPDATE waterings SET ended_at=?, duration_s=?, volume_l=?, end_reason=? WHERE req_id=?",
                  (ended_at, duration_s, volume_l, reason, req_id))

    def list_waterings(self, limit=50):
        return [dict(r) for r in self._run("SELECT * FROM waterings ORDER BY started_at DESC LIMIT ?", (limit,))]

    # ---- eventos (auditoría)
    def add_event(self, ts, level, source, message):
        self._run("INSERT INTO events(ts, level, source, message) VALUES(?,?,?,?)", (ts, level, source, message))
        self._run("DELETE FROM events WHERE id < (SELECT MAX(id) FROM events) - 5000")

    def recent_events(self, limit=100):
        rows = self._run("SELECT ts, level, source, message FROM events ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in reversed(rows)]
