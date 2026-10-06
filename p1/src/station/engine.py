"""
WM_WS_E - Watering Station Engine.
Usage: python engine.py <kafka_broker> <monitor_ip> <monitor_port>
The WS ID is passed to the Monitor and confirmed via Central's auth.
"""
import sys
import socket
import threading
import time
from datetime import datetime

sys.path.insert(0, '..')
from common.kafka_utils import (
    KafkaProducerWrapper, KafkaConsumerWrapper,
    TOPIC_COMMANDS, TOPIC_STATUS
)
from common import protocol as proto

# ---------- State ----------
producer = None
ws_id = None
state = 'IDLE'          # IDLE | WATERING | LEAK | BLOCKED
flow_rate = 10.0        # L/min simulated
accumulated = 0.0
start_ts = 0.0
duration_limit = 0
leak_flag = False
lock = threading.Lock()
monitor_conn = None


def watering_loop():
    """Simulate watering, send updates each second."""
    global accumulated, state, start_ts
    print(f"[WS_E:{ws_id}] Watering started")
    while True:
        time.sleep(1)
        with lock:
            if state != 'WATERING':
                break
            if leak_flag:
                state = 'LEAK'
                break
            elapsed = time.time() - start_ts
            accumulated += flow_rate / 60.0  # L per second
            if elapsed >= duration_limit:
                break
            # Send status
            producer.send(TOPIC_STATUS, {
                'ws_id': ws_id,
                'flow_rate': flow_rate,
                'volume': round(accumulated, 2),
                'elapsed': int(elapsed),
                'finished': False
            })
            print(f"[WS_E:{ws_id}] {flow_rate:.1f} L/min | {accumulated:.1f} L total")

    # Watering finished
    with lock:
        total = accumulated
        duration = int(time.time() - start_ts)
        state = 'IDLE'
        accumulated = 0.0
    producer.send(TOPIC_STATUS, {
        'ws_id': ws_id,
        'finished': True,
        'total_volume': round(total, 2),
        'duration': duration
    })
    print(f"[WS_E:{ws_id}] Watering ended. Total={total:.1f}L in {duration}s")


def handle_command(msg: dict):
    global state, start_ts, duration_limit, accumulated, leak_flag
    if msg.get('ws_id') != ws_id:
        return
    cmd = msg.get('command')
    print(f"[WS_E:{ws_id}] Command received: {cmd}")

    if cmd == 'START':
        with lock:
            if state in ('LEAK', 'BLOCKED'):
                print(f"[WS_E:{ws_id}] Cannot start, state={state}")
                return
            state = 'WATERING'
            start_ts = time.time()
            accumulated = 0.0
            duration_limit = msg.get('duration', 60)
            leak_flag = False
        threading.Thread(target=watering_loop, daemon=True).start()

    elif cmd == 'STOP':
        with lock:
            if state == 'WATERING':
                state = 'IDLE'
                # watering_loop will notice and send finished

    elif cmd == 'BLOCK':
        with lock:
            state = 'BLOCKED'
        print(f"[WS_E:{ws_id}] BLOCKED by Central")

    elif cmd == 'ACTIVATE':
        with lock:
            if state != 'LEAK':
                state = 'IDLE'
        print(f"[WS_E:{ws_id}] Activated")


# ---------- Socket server (accepts Monitor) ----------

def handle_monitor(conn, addr):
    global monitor_conn, leak_flag, state
    monitor_conn = conn
    print(f"[WS_E:{ws_id}] Monitor connected from {addr}")
    try:
        while True:
            frame = proto.read_frame(conn)
            if not frame:
                break
            if frame == proto.ENQ:
                conn.sendall(proto.ACK)
                continue
            payload, ok = proto.parse_frame(frame)
            if not ok:
                conn.sendall(proto.NACK)
                continue
            conn.sendall(proto.ACK)
            op, fields = proto.parse_request(payload)
            if op == 'HEARTBEAT':
                # Reply with status
                with lock:
                    reply = proto.build_request('STATUS', ws_id, state)
                conn.sendall(proto.build_frame(reply))
                conn.recv(1)  # ACK
            elif op == 'KO':
                # Leak simulated by monitor
                print(f"[WS_E:{ws_id}] *** KO received from Monitor - LEAK ***")
                with lock:
                    leak_flag = True
                    state = 'LEAK'
                # Notify Central (would normally be Monitor's job, but keep here too)
    except Exception as e:
        print(f"[WS_E:{ws_id}] Monitor socket error: {e}")
    finally:
        conn.close()


def socket_server(port: int):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('0.0.0.0', port))
    srv.listen(5)
    print(f"[WS_E:{ws_id}] Socket server for Monitor on port {port}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_monitor, args=(conn, addr), daemon=True).start()


def main():
    global producer, ws_id
    if len(sys.argv) < 4:
        print("Usage: python engine.py <kafka_broker> <monitor_ip> <monitor_port> [ws_id]")
        sys.exit(1)
    broker = sys.argv[1]
    # monitor_ip/port reserved
    # ws_id passed as optional 4th arg for convenience
    ws_id = sys.argv[4] if len(sys.argv) > 4 else input("WS ID: ").strip()

    producer = KafkaProducerWrapper(broker)
    consumer = KafkaConsumerWrapper(broker, f'ws-engine-{ws_id}', [TOPIC_COMMANDS])
    consumer.start_thread(handle_command)

    # Socket server on monitor_port to receive from Monitor
    threading.Thread(target=socket_server, args=(int(sys.argv[3]),), daemon=True).start()

    print(f"[WS_E:{ws_id}] Engine ready.")
    while True:
        time.sleep(1)


if __name__ == '__main__':
    main()
