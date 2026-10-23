"""RabbitMQ Management API (HTTP) orqali haqiqiy navbat metrikalari."""
import requests

from src.config import (
    DEAD_LETTER_QUEUE,
    RABBITMQ_HOST,
    RABBITMQ_MGMT_PORT,
    RABBITMQ_PASS,
    RABBITMQ_USER,
    TASK_QUEUES,
)


def _queue_stats(queue: str, timeout: float) -> dict:
    url = f"http://{RABBITMQ_HOST}:{RABBITMQ_MGMT_PORT}/api/queues/%2F/{queue}"
    response = requests.get(url, auth=(RABBITMQ_USER, RABBITMQ_PASS), timeout=timeout)
    if response.status_code == 404:  # Navbat hali e'lon qilinmagan (producer/worker ishga tushmagan)
        return {"length": 0, "consumers": 0, "ack_rate": 0.0}
    response.raise_for_status()
    data = response.json()
    ack_rate = data.get("message_stats", {}).get("ack_details", {}).get("rate", 0.0)
    return {
        "length": data.get("messages", 0),
        "consumers": data.get("consumers", 0),
        "ack_rate": ack_rate,
    }


def fetch_metrics(timeout: float = 3.0) -> dict:
    """Navbatlar uzunligi, ishchilar (consumer) soni va soniyasiga bajarilgan vazifalar.

    Ulanishda xatolik bo'lsa `requests.RequestException` ko'tariladi.
    """
    queues = {name: _queue_stats(name, timeout) for name in [*TASK_QUEUES, DEAD_LETTER_QUEUE]}
    task_stats = [queues[name] for name in TASK_QUEUES]
    return {
        "queues": queues,
        "workers": sum(s["consumers"] for s in task_stats),
        "processed_per_second": round(sum(s["ack_rate"] for s in task_stats), 2),
    }
