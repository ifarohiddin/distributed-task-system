import json
import uuid
import zlib

import pika

from src.config import EXCHANGE_NAME, TASK_QUEUES
from src import rabbit


def build_task(task_type, payload, priority=5) -> dict:
    return {
        "task_id": str(uuid.uuid4()),
        "type": task_type,
        "payload": payload,
        "priority": priority,
        "retry_count": 0,
    }


def encode_task(task: dict) -> bytes:
    """Vazifa JSON ko'rinishida `zlib` bilan siqiladi."""
    return zlib.compress(json.dumps(task).encode())


class Producer:
    def __init__(self):
        self.connection = rabbit.connect()
        self.channel = self.connection.channel()
        rabbit.declare_exchanges(self.channel)
        self.channel.confirm_delivery()  # Nashriyotchi tasdiqlari

        for queue in TASK_QUEUES:
            if rabbit.declare_task_queue(self.channel, queue) is None:
                self.channel = self.connection.channel()  # Kanalni qayta ochish
                self.channel.confirm_delivery()

    def submit_task(self, queue, task_type, payload, priority=5) -> str:
        if queue not in TASK_QUEUES:
            raise ValueError(f"Noma'lum navbat: {queue!r}. Mavjud navbatlar: {sorted(TASK_QUEUES)}")
        task = build_task(task_type, payload, priority)
        try:
            self.channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=queue,
                body=encode_task(task),
                properties=pika.BasicProperties(delivery_mode=2, priority=priority),  # Doimiy xabar
            )
        except Exception as exc:
            print(f"Failed to send task: {exc}")
            raise
        print(f"Task {task['task_id']} sent to {queue}")
        return task["task_id"]

    def close(self):
        self.connection.close()


if __name__ == "__main__":
    producer = Producer()
    producer.submit_task("image_processing", "resize", {"image": "sample.jpg"}, priority=8)
    producer.close()
