"""
SQLite database for WM_Central.
"""
import sqlite3
import threading
import os
DB_PATH = os.getenv('DB_PATH', 'wm_central.db')

# DB_PATH = 'wm_central.db'
_lock = threading.Lock()


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _lock:
        conn = get_conn()
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS operators (
                id TEXT PRIMARY KEY,
                name TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS stations (
                id TEXT PRIMARY KEY,
                location TEXT,
                state TEXT DEFAULT 'DISCONNECTED',
                current_operator TEXT,
                flow_rate REAL DEFAULT 0.0,
                accumulated_volume REAL DEFAULT 0.0,
                start_time TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS waterings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ws_id TEXT,
                operator_id TEXT,
                start_time TEXT,
                end_time TEXT,
                total_volume REAL,
                duration INTEGER
            )
        ''')
        conn.commit()
        conn.close()


def upsert_operator(op_id: str, name: str = None):
    with _lock:
        conn = get_conn()
        conn.execute(
            'INSERT OR IGNORE INTO operators (id, name) VALUES (?, ?)',
            (op_id, name or op_id)
        )
        conn.commit()
        conn.close()


def register_station(ws_id: str, location: str):
    with _lock:
        conn = get_conn()
        conn.execute(
            'INSERT OR IGNORE INTO stations (id, location) VALUES (?, ?)',
            (ws_id, location)
        )
        conn.commit()
        conn.close()


def update_station_state(ws_id: str, state: str, **kwargs):
    with _lock:
        conn = get_conn()
        fields = ['state = ?']
        values = [state]
        for k, v in kwargs.items():
            fields.append(f'{k} = ?')
            values.append(v)
        values.append(ws_id)
        conn.execute(
            f'UPDATE stations SET {", ".join(fields)} WHERE id = ?',
            values
        )
        conn.commit()
        conn.close()


def get_station(ws_id: str):
    with _lock:
        conn = get_conn()
        row = conn.execute('SELECT * FROM stations WHERE id = ?', (ws_id,)).fetchone()
        conn.close()
        return dict(row) if row else None


def get_all_stations():
    with _lock:
        conn = get_conn()
        rows = conn.execute('SELECT * FROM stations').fetchall()
        conn.close()
        return [dict(r) for r in rows]


def log_watering_start(ws_id: str, operator_id: str, start_time: str):
    with _lock:
        conn = get_conn()
        conn.execute(
            'INSERT INTO waterings (ws_id, operator_id, start_time) VALUES (?, ?, ?)',
            (ws_id, operator_id, start_time)
        )
        conn.commit()
        conn.close()


def log_watering_end(ws_id: str, end_time: str, total_volume: float, duration: int):
    with _lock:
        conn = get_conn()
        conn.execute(
            '''UPDATE waterings SET end_time = ?, total_volume = ?, duration = ?
               WHERE ws_id = ? AND end_time IS NULL''',
            (end_time, total_volume, duration, ws_id)
        )
        conn.commit()
        conn.close()
