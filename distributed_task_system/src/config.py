import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_MGMT_PORT = int(os.getenv("RABBITMQ_MGMT_PORT", "15672"))  # Management API (metrikalar uchun)

# Navbatlar va almashuvlar
EXCHANGE_NAME = "tasks_exchange"
TASK_QUEUES = {
    "image_processing": {"priority": True, "max_priority": 10},
    "data_analysis": {"priority": True, "max_priority": 10}
}
DEAD_LETTER_EXCHANGE = "dlx_exchange"
DEAD_LETTER_QUEUE = "dead_letter_queue"

# Vazifa sozlamalari
MAX_RETRIES = 3
RETRY_BACKOFF = [1, 2, 4]  # Eksponensial kechikish (sekundlarda)
PREFETCH_COUNT = 10  # Har bir ishchi uchun oldindan olish chegarasi
