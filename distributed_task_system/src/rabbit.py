"""RabbitMQ bilan ishlash uchun umumiy yordamchilar (ulanish va navbatlarni e'lon qilish)."""
import pika

from src.config import (
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_QUEUE,
    EXCHANGE_NAME,
    RABBITMQ_HOST,
    RABBITMQ_PASS,
    RABBITMQ_PORT,
    RABBITMQ_USER,
    TASK_QUEUES,
)


def connect() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    )


def queue_arguments(queue: str) -> dict:
    """Navbat sozlamalari: ustuvorlik (agar yoqilgan bo'lsa) va o'lik xabarlar almashuvi."""
    settings = TASK_QUEUES[queue]
    args = {"x-dead-letter-exchange": DEAD_LETTER_EXCHANGE}
    if settings["priority"]:
        args["x-max-priority"] = settings["max_priority"]
    return args


def declare_exchanges(channel) -> None:
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="direct")
    channel.exchange_declare(exchange=DEAD_LETTER_EXCHANGE, exchange_type="fanout")


def declare_dead_letter_queue(channel) -> None:
    channel.queue_declare(queue=DEAD_LETTER_QUEUE, durable=True)
    channel.queue_bind(queue=DEAD_LETTER_QUEUE, exchange=DEAD_LETTER_EXCHANGE)


def declare_task_queue(channel, queue: str):
    """Navbatni e'lon qiladi va almashuvga bog'laydi.

    Navbat boshqa argumentlar bilan allaqachon mavjud bo'lsa, broker kanalni yopadi;
    u holda `None` qaytariladi va chaqiruvchi yangi kanal ochib, shu navbatni o'zgartirmay
    ishlashda davom etadi. Muvaffaqiyatli bo'lsa, o'sha kanalning o'zi qaytariladi.
    """
    try:
        channel.queue_declare(queue=queue, durable=True, arguments=queue_arguments(queue))
        channel.queue_bind(queue=queue, exchange=EXCHANGE_NAME, routing_key=queue)
        return channel
    except pika.exceptions.ChannelClosedByBroker as exc:
        print(f"Queue {queue} already exists with different args: {exc}. Continuing...")
        return None
