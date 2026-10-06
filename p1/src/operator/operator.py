"""
WM_FO - Field Operator App.
Usage: python operator.py <kafka_broker> <operator_id> [services_file]
"""
import sys
import time
from datetime import datetime

sys.path.insert(0, '..')
from common.kafka_utils import (
    KafkaProducerWrapper, KafkaConsumerWrapper,
    TOPIC_REQUESTS, TOPIC_NOTIFICATIONS
)

operator_id = None
producer = None
pending_ws = None  # ws_id currently waiting for reply
pending_evt = None  # threading.Event


def load_services(path):
    """File format: WS_ID#duration_seconds per line."""
    services = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split('#')
            if len(parts) >= 2:
                services.append((parts[0], int(parts[1])))
    return services


def send_request(ws_id, duration):
    global pending_ws, pending_evt
    pending_ws = ws_id
    pending_evt = __import__('threading').Event()
    print(f"\n[FO:{operator_id}] Requesting {ws_id} for {duration}s")
    producer.send(TOPIC_REQUESTS, {
        'operator_id': operator_id,
        'ws_id': ws_id,
        'duration': duration,
        'timestamp': datetime.now().isoformat()
    })
    # Wait up to 10s for auth reply
    pending_evt.wait(timeout=10.0)
    return pending_ws is None  # cleared by notification handler


def handle_notification(msg: dict):
    """Notifications from Central."""
    if msg.get('operator_id') != operator_id:
        return
    status = msg.get('status')
    ws_id = msg.get('ws_id')
    detail = msg.get('detail', '')
    print(f"\n[FO:{operator_id}] <<< {status} for {ws_id}: {detail}")

    global pending_ws, pending_evt
    if status in ('AUTHORIZED', 'DENIED'):
        pending_ws = None
        if pending_evt:
            pending_evt.set()


def main():
    global operator_id, producer
    if len(sys.argv) < 3:
        print("Usage: python operator.py <kafka_broker> <operator_id> [services_file]")
        sys.exit(1)
    broker = sys.argv[1]
    operator_id = sys.argv[2]
    services_file = sys.argv[3] if len(sys.argv) > 3 else 'services.txt'

    producer = KafkaProducerWrapper(broker)
    consumer = KafkaConsumerWrapper(
        broker, f'fo-{operator_id}', [TOPIC_NOTIFICATIONS]
    )
    consumer.start_thread(handle_notification)

    print(f"[FO:{operator_id}] Starting. Reading {services_file}")
    try:
        services = load_services(services_file)
    except FileNotFoundError:
        print(f"File {services_file} not found. Manual mode.")
        services = []

    for ws_id, duration in services:
        send_request(ws_id, duration)
        # Wait for the watering to finish (up to duration + 5s)
        print(f"[FO:{operator_id}] Waiting for {ws_id} to finish...")
        time.sleep(duration + 5)
        print(f"[FO:{operator_id}] Service done. Waiting 4s before next.")
        time.sleep(4)

    print("[FO] All services processed. Entering manual mode.")
    while True:
        line = input("WS_ID#duration (or 'q' to quit): ").strip()
        if line.lower() == 'q':
            break
        parts = line.split('#')
        if len(parts) == 2:
            send_request(parts[0], int(parts[1]))


if __name__ == '__main__':
    main()
