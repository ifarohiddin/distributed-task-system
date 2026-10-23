"""Haqiqiy RabbitMQ bilan to'liq oqimni tekshiradi (pytest uni avtomatik yig'maydi).

Talab: RabbitMQ (management plagini bilan) localhost'da ishlab turishi kerak, masalan:
    docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management
Ishga tushirish (distributed_task_system/ papkasidan):
    python tests/e2e_rabbitmq.py
Diqqat: skript `image_processing`, `data_analysis` va `dead_letter_queue` navbatlaridan foydalanadi,
shuning uchun faqat test brokerida ishlating.
"""
import sys, time, threading, json, zlib
sys.path.insert(0, ".")
from src import worker as wmod, rabbit
from src.producer import Producer
from src.client import TaskClient
from src.config import DEAD_LETTER_QUEUE, MAX_RETRIES

wmod.RETRY_BACKOFF = [0.05, 0.05, 0.05]  # testni tezlashtirish
ok = True


def check(name, cond, extra=""):
    global ok
    ok &= bool(cond)
    print(("OK   " if cond else "XATO ") + name, extra, flush=True)


def count(queue):
    import pika
    conn = rabbit.connect()
    try:
        ch = conn.channel()
        return ch.queue_declare(queue=queue, passive=True).method.message_count
    except pika.exceptions.ChannelClosedByBroker:  # navbat hali yaratilmagan
        return 0
    finally:
        conn.close()


def run_worker(queue, callback, until, timeout=10):
    """Worker'ni alohida oqimda ishga tushiradi; `until()` rost bo'lganda to'xtatadi."""
    holder = {}

    def target():
        w = wmod.Worker(queue, callback)
        holder["w"] = w
        try:
            w.start()
        except Exception:
            pass

    t = threading.Thread(target=target, daemon=True)
    t.start()
    start = time.time()
    while time.time() - start < timeout and not until():
        time.sleep(0.05)
    w = holder["w"]
    w.connection.add_callback_threadsafe(lambda: w.channel.stop_consuming())
    t.join(5)
    try:
        w.close()
    except Exception:
        pass


# --- 1. har doim xato beradigan callback: 1 + MAX_RETRIES urinish, so'ng DLQ ga 1 ta xabar
p = Producer()
tid = p.submit_task("image_processing", "resize", {"image": "a.jpg"}, priority=8)
p.close()
calls = []


def always_fail(task):
    calls.append(task["retry_count"])
    raise RuntimeError("doim xato")


run_worker("image_processing", always_fail,
           lambda: count(DEAD_LETTER_QUEUE) >= 1 and len(calls) >= MAX_RETRIES + 1)
time.sleep(0.3)
check("har doim xato: urinishlar soni = 1 + MAX_RETRIES", calls == [0, 1, 2, 3], f"retry_count ketma-ketligi={calls}")
check("asosiy navbat bo'sh", count("image_processing") == 0)
check("DLQ da aynan 1 ta xabar (dublikatlarsiz)", count(DEAD_LETTER_QUEUE) == 1, f"DLQ={count(DEAD_LETTER_QUEUE)}")

# --- 2. DLQ dan qayta yuborish
c = TaskClient()
moved = c.retry_dead_letters()
check("retry_dead_letters 1 ta vazifani qaytardi", moved == 1, f"moved={moved}")
check("DLQ bo'shadi, asosiy navbatda 1 ta", count(DEAD_LETTER_QUEUE) == 0 and count("image_processing") == 1)
method, props, body = c.channel.basic_get("image_processing", auto_ack=True)
task = json.loads(zlib.decompress(body).decode())
check("qaytgan vazifada task_id saqlangan, retry_count=0, priority=8",
      task["task_id"] == tid and task["retry_count"] == 0 and props.priority == 8,
      f"{task['task_id'][:8]} rc={task['retry_count']} prio={props.priority}")

# --- 3. 2 marta xato beradi, 3-urinishda muvaffaqiyat
p = Producer()
p.submit_task("data_analysis", "report", {"n": 1})
p.close()
attempts = []


def flaky(task):
    attempts.append(task["retry_count"])
    if len(attempts) < 3:
        raise RuntimeError("vaqtincha xato")


run_worker("data_analysis", flaky, lambda: len(attempts) >= 3)
time.sleep(0.3)
check("flaky: 3 urinishdan so'ng muvaffaqiyat", attempts == [0, 1, 2], f"{attempts}")
check("flaky: navbat bo'sh va DLQ ga tushmadi", count("data_analysis") == 0 and count(DEAD_LETTER_QUEUE) == 0)

# --- 4. buzuq xabar worker'ni yiqitmaydi va DLQ ga tushadi
conn = rabbit.connect()
ch = conn.channel()
ch.basic_publish(exchange="tasks_exchange", routing_key="image_processing", body=b"buzuq-xabar")
conn.close()
seen = []
run_worker("image_processing", seen.append, lambda: count(DEAD_LETTER_QUEUE) >= 1)
check("buzuq xabar DLQ ga tushdi, callback chaqirilmadi", count(DEAD_LETTER_QUEUE) == 1 and seen == [])

# --- 5. metrikalar (haqiqiy Management API)
p = Producer()
for i in range(3):
    p.submit_task("data_analysis", "report", {"i": i})
p.close()
time.sleep(6)  # management statistikasi ~5s da yangilanadi
m = c.get_metrics()
check("metrikalar: data_analysis uzunligi 3", m["queues"]["data_analysis"]["length"] == 3,
      json.dumps(m["queues"]["data_analysis"]))
check("metrikalar: DLQ uzunligi 1", m["queues"][DEAD_LETTER_QUEUE]["length"] == 1)
check("metrikalar: ishchilar 0 (hech kim tinglamayapti)", m["workers"] == 0)
from src.monitor import app
resp = app.test_client().get("/metrics")
check("monitor /metrics 200 va haqiqiy ma'lumot",
      resp.status_code == 200 and resp.get_json()["queues"]["data_analysis"]["length"] == 3)
c.close()
print("\nNATIJA:", "HAMMASI O'TDI" if ok else "XATOLAR BOR")
