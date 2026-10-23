import json
import zlib

import pika

from src.config import DEAD_LETTER_QUEUE, EXCHANGE_NAME, TASK_QUEUES
from src import rabbit
from src.metrics import fetch_metrics
from src.producer import build_task, encode_task
from src.worker import Worker


class TaskClient:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        self.connection = rabbit.connect()
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")
        self.channel.confirm_delivery()

    def _ensure_connected(self):
        if not self.connection or self.connection.is_closed:
            self.connect()

    def submit_task(self, queue, task_type, payload, priority=5) -> str:
        """Vazifani navbatga yuboradi va uning `task_id` sini qaytaradi."""
        if queue not in TASK_QUEUES:
            raise ValueError(f"Noma'lum navbat: {queue!r}. Mavjud navbatlar: {sorted(TASK_QUEUES)}")
        self._ensure_connected()
        task = build_task(task_type, payload, priority)
        self.channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=queue,
            body=encode_task(task),
            properties=pika.BasicProperties(delivery_mode=2, priority=priority),
        )
        return task["task_id"]

    def start_worker(self, queue, callback):
        Worker(queue, callback).start()

    @staticmethod
    def _source_queue(properties):
        """Broker o'lik xabarga qo'shgan `x-death` sarlavhasidan asl navbat nomini oladi."""
        deaths = (getattr(properties, "headers", None) or {}).get("x-death") or []
        return deaths[0].get("queue") if deaths else None

    def retry_dead_letters(self, limit=100) -> int:
        """`dead_letter_queue` dagi vazifalarni asl navbatiga qaytaradi (`retry_count` nolga tushadi).

        Asl navbatini aniqlab bo'lmaydigan yoki buzuq xabarlar navbatda qoladi.
        Qayta navbatga qo'yilgan vazifalar sonini qaytaradi.
        """
        self._ensure_connected()
        requeued, skipped = 0, []
        while requeued + len(skipped) < limit:
            method, properties, body = self.channel.basic_get(queue=DEAD_LETTER_QUEUE, auto_ack=False)
            if method is None:
                break
            queue = self._source_queue(properties)
            try:
                task = json.loads(zlib.decompress(body).decode())
            except (zlib.error, ValueError):
                task = None
            if task is None or queue not in TASK_QUEUES:
                skipped.append(method.delivery_tag)
                continue
            task["retry_count"] = 0
            self.channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=queue,
                body=zlib.compress(json.dumps(task).encode()),
                properties=pika.BasicProperties(delivery_mode=2, priority=task.get("priority", 5)),
            )
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
            requeued += 1
        for tag in skipped:
            self.channel.basic_nack(delivery_tag=tag, requeue=True)
        return requeued

    def get_metrics(self) -> dict:
        """RabbitMQ Management API'dan haqiqiy metrikalar (qarang: `src.metrics`)."""
        return fetch_metrics()

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
