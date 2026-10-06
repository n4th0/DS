"""
WM_Central - Central control system.
Usage: python central.py <socket_port> <kafka_broker>
"""
import sys
import socket
import threading
import json
import time
from datetime import datetime

sys.path.insert(0, '..')
from common import database as db
from common import protocol as proto
from common.kafka_utils import (
    KafkaProducerWrapper, KafkaConsumerWrapper,
    TOPIC_REQUESTS, TOPIC_COMMANDS, TOPIC_STATUS, TOPIC_NOTIFICATIONS
)

# ------------ Global state ------------
producer = None
stations_lock = threading.Lock()


# ------------ Kafka callbacks ------------

def handle_fo_request(msg: dict):
    """Message from Field Operator: {operator_id, ws_id, duration}"""
    op_id = msg.get('operator_id')
    ws_id = msg.get('ws_id')
    duration = msg.get('duration', 60)

    print(f"\n[CENTRAL] Request from {op_id} to water {ws_id} for {duration}s")

    station = db.get_station(ws_id)
    if not station:
        notify_fo(op_id, ws_id, 'DENIED', 'Station not registered')
        return

    if station['state'] != 'AVAILABLE':
        notify_fo(op_id, ws_id, 'DENIED', f"Station is {station['state']}")
        return

    # Authorize
    start_time = datetime.now().isoformat()
    db.update_station_state(
        ws_id, 'WATERING',
        current_operator=op_id,
        flow_rate=0.0,
        accumulated_volume=0.0,
        start_time=start_time
    )
    db.log_watering_start(ws_id, op_id, start_time)

    producer.send(TOPIC_COMMANDS, {
        'command': 'START',
        'ws_id': ws_id,
        'operator_id': op_id,
        'duration': duration
    })

    notify_fo(op_id, ws_id, 'AUTHORIZED', f'Watering for {duration}s')
    print(f"[CENTRAL] {ws_id} -> WATERING (operator {op_id})")


def handle_ws_status(msg: dict):
    """Status update from WS_E: {ws_id, flow_rate, volume, elapsed, finished, total_volume}"""
    ws_id = msg.get('ws_id')
    station = db.get_station(ws_id)
    if not station:
        return

    if msg.get('finished'):
        total = msg.get('total_volume', 0.0)
        duration = msg.get('duration', 0)
        op_id = station.get('current_operator')
        db.update_station_state(
            ws_id, 'AVAILABLE',
            current_operator=None, flow_rate=0.0, accumulated_volume=0.0
        )
        db.log_watering_end(ws_id, datetime.now().isoformat(), total, duration)
        if op_id:
            notify_fo(op_id, ws_id, 'FINISHED',
                      f'Total: {total:.1f}L in {duration}s')
        print(f"[CENTRAL] {ws_id} finished. Total {total:.1f}L")
    else:
        # Real-time update
        db.update_station_state(
            ws_id, 'WATERING',
            flow_rate=msg.get('flow_rate', 0.0),
            accumulated_volume=msg.get('volume', 0.0)
        )


def notify_fo(operator_id: str, ws_id: str, status: str, detail: str):
    producer.send(TOPIC_NOTIFICATIONS, {
        'operator_id': operator_id,
        'ws_id': ws_id,
        'status': status,
        'detail': detail,
        'timestamp': datetime.now().isoformat()
    })


# ------------ Socket server for WM_WS_M ------------

def handle_monitor_client(conn, addr):
    print(f"[CENTRAL] Monitor connected from {addr}")
    try:
        while True:
            frame = proto.read_frame(conn)
            if not frame:
                break
            if frame in (proto.ENQ,):
                conn.sendall(proto.ACK)
                continue

            payload, ok = proto.parse_frame(frame)
            if not ok:
                conn.sendall(proto.NACK)
                continue
            conn.sendall(proto.ACK)

            op, fields = proto.parse_request(payload)
            print(f"[CENTRAL] Socket op={op} fields={fields}")

            if op == 'REG':
                # REG#WS_ID#LOCATION
                ws_id, location = fields[0], fields[1]
                db.register_station(ws_id, location)
                db.update_station_state(ws_id, 'AVAILABLE')
                answer = proto.build_request('REG_OK', ws_id)
                send_answer(conn, answer)

            elif op == 'HEARTBEAT':
                # HEARTBEAT#WS_ID
                ws_id = fields[0]
                station = db.get_station(ws_id)
                if station and station['state'] == 'DISCONNECTED':
                    db.update_station_state(ws_id, 'AVAILABLE')
                answer = proto.build_request('HB_OK', ws_id)
                send_answer(conn, answer)

            elif op == 'LEAK':
                # LEAK#WS_ID
                ws_id = fields[0]
                db.update_station_state(ws_id, 'LEAK', flow_rate=0.0)
                print(f"[CENTRAL] *** LEAK detected on {ws_id} ***")
                # Force stop any watering
                producer.send(TOPIC_COMMANDS, {'command': 'STOP', 'ws_id': ws_id})
                station = db.get_station(ws_id)
                if station and station['current_operator']:
                    notify_fo(station['current_operator'], ws_id,
                              'LEAK', 'Leak detected, watering aborted')
                answer = proto.build_request('LEAK_ACK', ws_id)
                send_answer(conn, answer)

            elif op == 'LEAK_RESOLVED':
                ws_id = fields[0]
                db.update_station_state(ws_id, 'AVAILABLE')
                print(f"[CENTRAL] Leak resolved on {ws_id}")
                answer = proto.build_request('OK', ws_id)
                send_answer(conn, answer)

            else:
                send_answer(conn, proto.build_request('ERROR', 'UNKNOWN_OP'))
    except Exception as e:
        print(f"[CENTRAL] Socket error with {addr}: {e}")
    finally:
        conn.close()
        print(f"[CENTRAL] Monitor {addr} disconnected")


def send_answer(conn, payload: str):
    conn.sendall(proto.build_frame(payload))
    # wait for ACK
    ack = conn.recv(1)
    if ack != proto.ACK:
        print("[CENTRAL] Warning: no ACK for answer")


def socket_server(port: int):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('0.0.0.0', port))
    srv.listen(10)
    print(f"[CENTRAL] Socket server listening on 0.0.0.0:{port}")
    while True:
        conn, addr = srv.accept()
        t = threading.Thread(target=handle_monitor_client, args=(conn, addr), daemon=True)
        t.start()


# ------------ Console menu (Point 11) ------------

def console_menu():
    while True:
        print("\n=== CENTRAL MENU ===")
        print("1. Start watering on a WS")
        print("2. Block WS (Out of Service)")
        print("3. Activate WS (Available)")
        print("4. Show all stations")
        opt = input("Option: ").strip()

        if opt == '1':
            ws_id = input("WS ID: ").strip()
            op_id = input("Operator ID: ").strip()
            handle_fo_request({'operator_id': op_id, 'ws_id': ws_id, 'duration': 60})

        elif opt == '2':
            ws_id = input("WS ID: ").strip()
            db.update_station_state(ws_id, 'OUT_OF_SERVICE')
            producer.send(TOPIC_COMMANDS, {'command': 'BLOCK', 'ws_id': ws_id})
            print(f"[CENTRAL] {ws_id} blocked")

        elif opt == '3':
            ws_id = input("WS ID: ").strip()
            db.update_station_state(ws_id, 'AVAILABLE')
            producer.send(TOPIC_COMMANDS, {'command': 'ACTIVATE', 'ws_id': ws_id})
            print(f"[CENTRAL] {ws_id} activated")

        elif opt == '4':
            for s in db.get_all_stations():
                print(s)


# ------------ Main ------------

def main():
    global producer
    if len(sys.argv) < 3:
        print("Usage: python central.py <socket_port> <kafka_broker>")
        sys.exit(1)

    port = int(sys.argv[1])
    broker = sys.argv[2]

    db.init_db()
    db.upsert_operator('FO-01')
    db.upsert_operator('FO-02')

    producer = KafkaProducerWrapper(broker)

    # Kafka consumers
    req_consumer = KafkaConsumerWrapper(broker, 'central-requests', [TOPIC_REQUESTS])
    req_consumer.start_thread(handle_fo_request)

    status_consumer = KafkaConsumerWrapper(broker, 'central-status', [TOPIC_STATUS])
    status_consumer.start_thread(handle_ws_status)

    # Socket server thread
    threading.Thread(target=socket_server, args=(port,), daemon=True).start()

    # Menu in main thread
    time.sleep(1)
    console_menu()


if __name__ == '__main__':
    main()
