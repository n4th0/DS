"""
Kafka helpers for Water Management.
"""
import json
import threading
from confluent_kafka import Producer, Consumer, KafkaError

# Topics
TOPIC_REQUESTS = 'wm_requests'          # FO -> Central
TOPIC_COMMANDS = 'wm_commands'          # Central -> WS_E
TOPIC_STATUS = 'wm_status'              # WS_E -> Central
TOPIC_NOTIFICATIONS = 'wm_notifications'  # Central -> FO


class KafkaProducerWrapper:
    def __init__(self, broker):
        self.producer = Producer({'bootstrap.servers': broker})

    def send(self, topic: str, message: dict):
        self.producer.produce(
            topic,
            value=json.dumps(message).encode('utf-8'),
            callback=self._delivery_report
        )
        self.producer.poll(0)

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"[KAFKA] Delivery failed: {err}")

    def flush(self):
        self.producer.flush()


class KafkaConsumerWrapper:
    def __init__(self, broker, group_id, topics):
        self.consumer = Consumer({
            'bootstrap.servers': broker,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        })
        self.consumer.subscribe(topics)
        self.running = True

    def poll_loop(self, callback):
        """Run a blocking loop, passing each decoded message to callback."""
        while self.running:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[KAFKA] Error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                callback(data)
            except Exception as e:
                print(f"[KAFKA] Parse error: {e}")

    def start_thread(self, callback):
        t = threading.Thread(target=self.poll_loop, args=(callback,), daemon=True)
        t.start()
        return t

    def stop(self):
        self.running = False
        self.consumer.close()
