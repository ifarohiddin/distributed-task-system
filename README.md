# 🐇 Distributed Task Processing System

**RabbitMQ** asosidagi taqsimlangan vazifalarni qayta ishlash tizimi. Producer vazifalarni navbatga yuboradi, bir nechta worker ularni parallel bajaradi. Priority navbatlar, qayta urinish (retry) va dead-letter navbati qo'llab-quvvatlanadi.

## ✨ Imkoniyatlar

- 📬 Priority (1–10) navbatlar: `image_processing`, `data_analysis`
- 🔁 Xatolikda qayta urinish (exponential backoff: 1s, 2s, 4s; maks. 3 marta)
- ☠️ Dead-letter exchange / queue (bajarilmagan vazifalar uchun)
- ✅ Publisher confirm va xabarlarni `zlib` bilan siqish
- 📈 Flask'da yozilgan monitoring endpointi (`/metrics`)
- 🐳 Docker Compose bilan bir buyruqda ishga tushadi (RabbitMQ + producer + 2 worker)
- 🧪 `pytest` unit va stress testlari

## 🛠️ Texnologiyalar

`Python` · `RabbitMQ (pika)` · `Docker / Docker Compose` · `Flask` · `pytest`

## 🏗️ Arxitektura

```text
Client ──► Producer ──► tasks_exchange ──► [image_processing | data_analysis]
                                                    │
                                          Worker (x2, prefetch=10)
                                                    │
                                     xatolik ──► retry ──► dead_letter_queue
```

## 🚀 Ishga tushirish

```bash
git clone https://github.com/ifarohiddin/distributed-task-system.git
cd distributed-task-system/distributed_task_system

docker compose up --build
```

RabbitMQ boshqaruv paneli: <http://localhost:15672> (login/parol: `guest` / `guest`, faqat lokal ishlatish uchun).

### Ishlatish

```python
from src.client import TaskClient

client = TaskClient()
task_id = client.submit_task("image_processing", "resize", {"image": "sample.jpg"}, priority=8)
metrics = client.get_metrics()
```

### Testlar

```bash
pip install -r requirements.txt
pytest tests/
```

## 📁 Tuzilma

```text
distributed_task_system/
├── src/
│   ├── producer.py   # vazifalarni navbatga yuboradi
│   ├── worker.py     # vazifalarni bajaradi, retry/DLQ
│   ├── client.py     # TaskClient
│   ├── monitor.py    # Flask /metrics
│   ├── config.py
│   └── utils.py
├── tests/
├── docs/design.md
├── Dockerfile
└── docker-compose.yml

examples/
└── restaurant_simulation/   # buyurtma → oshpaz → bildirishnoma → yetkazib berish
```

## 🍕 Qo'shimcha namuna: `examples/restaurant_simulation`

RabbitMQ'da oddiy mikroservislar zanjiri: **buyurtma → oshpaz → bildirishnoma → yetkazib berish**. Bildirishnoma va yetkazib berish bosqichlarida tasodifiy xatolik simulyatsiya qilinadi va xabar `nack` bilan qayta navbatga qo'yiladi.

```bash
cd examples/restaurant_simulation
pip install -r requirements.txt
# RabbitMQ localhost'da ishlab turishi kerak. Har birini alohida terminalda:
python chef_service.py
python notification_service.py
python delivery_service.py
python order_creator.py
```

## 🌱 Rejalar

- [ ] `monitor.py` metrikalarini haqiqiy RabbitMQ Management API'dan olish
- [x] RabbitMQ login/paroli muhit o'zgaruvchilaridan o'qiladi (`RABBITMQ_USER`, `RABBITMQ_PASS`, `RABBITMQ_PORT`)
- [ ] GitHub Actions bilan testlarni avtomatlashtirish

## 👤 Muallif

**Isomidinov Farohiddin** — [@ifarohiddin](https://github.com/ifarohiddin)
