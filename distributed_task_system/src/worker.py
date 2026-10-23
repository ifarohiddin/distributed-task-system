import json
import time
import zlib

from src.config import (
    EXCHANGE_NAME,
    MAX_RETRIES,
    PREFETCH_COUNT,
    RETRY_BACKOFF,
    TASK_QUEUES,
)
from src import rabbit


class Worker:
    def __init__(self, queue, callback):
        if queue not in TASK_QUEUES:
            raise ValueError(f"Noma'lum navbat: {queue!r}. Mavjud navbatlar: {sorted(TASK_QUEUES)}")
        self.connection = rabbit.connect()
        self.channel = self.connection.channel()
        rabbit.declare_exchanges(self.channel)
        rabbit.declare_dead_letter_queue(self.channel)
        if rabbit.declare_task_queue(self.channel, queue) is None:
            self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
        self.callback = callback
        self.queue = queue

    def process_task(self, ch, method, properties, body):
        """Bitta vazifani bajaradi.

        - Muvaffaqiyat: `ack`.
        - Xatolik va urinishlar tugamagan: vazifaning yangi nusxasi (`retry_count` + 1) qayta
          navbatga qo'yiladi, asl xabar `ack` qilinadi (u o'lik xabarlar navbatiga tushmaydi).
        - Urinishlar tugadi yoki xabar buzuq: `nack(requeue=False)`, broker xabarni
          navbatdagi `x-dead-letter-exchange` orqali `dead_letter_queue` ga yo'naltiradi.
        """
        try:
            task = json.loads(zlib.decompress(body).decode())
        except (zlib.error, ValueError) as exc:  # UnicodeDecodeError ham ValueError
            print(f"Invalid message, sending to dead letter queue: {exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        try:
            self.callback(task)
        except Exception as exc:
            print(f"Error processing task {task.get('task_id')}: {exc}")
            retry_count = task.get("retry_count", 0) + 1
            if retry_count > MAX_RETRIES:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            task["retry_count"] = retry_count
            time.sleep(RETRY_BACKOFF[retry_count - 1])  # Eksponensial kechikish
            ch.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=self.queue,
                body=zlib.compress(json.dumps(task).encode()),
                properties=properties,
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.process_task)
        print(f"Worker started for {self.queue}")
        self.channel.start_consuming()

    def close(self):
        self.connection.close()


def example_callback(task):
    print(f"Processing task {task['task_id']} of type {task['type']}")
    time.sleep(0.01)  # Simulyatsiya


if __name__ == "__main__":
    worker = Worker("image_processing", example_callback)
    try:
        worker.start()
    except KeyboardInterrupt:
        worker.close()
