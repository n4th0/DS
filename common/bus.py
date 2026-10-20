"""
Capa de mensajería. KafkaBus (producción) y MemoryBus (tests sin broker)
"""
import json
import queue
import threading
import time
import traceback

from wm_common import topics as T
from wm_common.util import say


class Bus:
    def publish(self, topic, key, value):
        raise NotImplementedError

    def subscribe(self, topics, handler, group_id, from_start=False):
        """handler(topic, key, dict) devuelve un event que se activa al tener particion asignada"""
        raise NotImplementedError

    def describe(self):
        return "bus"

    def close(self):
        pass


# kafka
class KafkaBus(Bus):
    def __init__(self, bootstrap, client_id="wm"):
        from confluent_kafka import Producer
        self.bootstrap = bootstrap
        self.client_id = client_id
        self._stop = threading.Event()
        self._wait_for_broker_and_topics()
        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": client_id,
            "linger.ms": 5,
            "socket.keepalive.enable": True,
            "message.timeout.ms": 15000,
        })
        threading.Thread(target=self._poll_loop, daemon=True).start()
        say("KAFKA", f"Conectado al broker {bootstrap}")

    def describe(self):
        return f"kafka {self.bootstrap}"

    def _wait_for_broker_and_topics(self):
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": self.bootstrap, "socket.timeout.ms": 10000})
        existing = {}
        while not self._stop.is_set():
            try:
                existing = admin.list_topics(timeout=10).topics
                break
            except Exception as e:
                say("KAFKA", f"Esperando al broker {self.bootstrap} ({e}) ...")
                time.sleep(3)
        missing = [NewTopic(t, num_partitions=1, replication_factor=1)
                   for t in T.ALL_TOPICS if t not in existing]
        if missing:
            for topic, fut in admin.create_topics(missing).items():
                try:
                    fut.result(timeout=15)
                    say("KAFKA", f"Topic creado: {topic}")
                except Exception as e:  # ya existe (carrera entre módulos) u otro motivo
                    say("KAFKA", f"Topic {topic}: {e}")

    def _poll_loop(self):
        while not self._stop.is_set():
            self._producer.poll(0.2)

    def publish(self, topic, key, value):
        value = dict(value)
        value.setdefault("ts", time.time())
        payload = json.dumps(value).encode("utf-8")
        k = key.encode("utf-8") if key else None
        for _ in range(5):
            try:
                self._producer.produce(topic, key=k, value=payload, on_delivery=self._on_delivery)
                return
            except BufferError:
                time.sleep(0.2)
        say("KAFKA", f"Cola de producción llena, mensaje descartado ({topic})")

    @staticmethod
    def _on_delivery(err, msg):
        if err is not None:
            say("KAFKA", f"Error entregando a {msg.topic()}: {err}")

    def subscribe(self, topics, handler, group_id, from_start=False):
        ready = threading.Event()
        threading.Thread(target=self._consume_loop, daemon=True,
                         args=(list(topics), handler, group_id, from_start, ready)).start()
        return ready

    def _consume_loop(self, topics, handler, group_id, from_start, ready):
        from confluent_kafka import Consumer, KafkaError
        consumer = Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": group_id,
            "client.id": f"{self.client_id}-{group_id}",
            "auto.offset.reset": "earliest" if from_start else "latest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 2000,
            "session.timeout.ms": 10000,
            "socket.keepalive.enable": True,
        })
        consumer.subscribe(topics, on_assign=lambda c, parts: ready.set())
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() not in (KafkaError._PARTITION_EOF,
                                                  KafkaError.UNKNOWN_TOPIC_OR_PART):
                        say("KAFKA", f"Error de consumidor: {msg.error()}")
                    continue
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    key = msg.key().decode("utf-8") if msg.key() else None
                    handler(msg.topic(), key, value)
                except Exception:
                    say("KAFKA", "Error procesando mensaje:\n" + traceback.format_exc())
        finally:
            consumer.close()

    def close(self):
        self._stop.set()
        try:
            self._producer.flush(3)
        except Exception:
            pass


# memoria
class MemoryBus(Bus):
    """Bus en memoria (un solo proceso). Cada suscripcion recibe todos los mensajes de sus topics."""
    _shared = {}

    @classmethod
    def shared(cls, name):
        return cls._shared.setdefault(name, cls())

    def __init__(self):
        self._subs = []
        self._lock = threading.Lock()

    def describe(self):
        return "memory"

    def publish(self, topic, key, value):
        value = dict(value)
        value.setdefault("ts", time.time())
        value = json.loads(json.dumps(value))
        with self._lock:
            subs = [s for s in self._subs if topic in s[0]]
        for _, q in subs:
            q.put((topic, key, value))

    def subscribe(self, topics, handler, group_id, from_start=False):
        q = queue.Queue()
        with self._lock:
            self._subs.append((set(topics), q))

        def loop():
            while True:
                topic, key, value = q.get()
                try:
                    handler(topic, key, value)
                except Exception:
                    say("MEMBUS", traceback.format_exc())
        threading.Thread(target=loop, daemon=True).start()
        ev = threading.Event()
        ev.set()
        return ev


def get_bus(broker, client_id="wm"):
    if broker.startswith("memory"):
        return MemoryBus.shared(broker)
    return KafkaBus(broker, client_id)
