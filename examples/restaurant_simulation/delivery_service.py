import pika
import json
import time
import random

def callback(ch, method, properties, body):
    order = json.loads(body)
    print(f"[DeliveryService] Delivery guy approaching for Order: {order['order_id']}")
    time.sleep(2)

    # Retry mechanism
    if random.random() < 0.2:  # 20% chance of failure
        print("[DeliveryService] Delivery guy unavailable, retrying...")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    print(f"[DeliveryService] Order delivered: {order['order_id']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='delivery_queue')
channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue='delivery_queue', on_message_callback=callback)
print('[DeliveryService] Waiting for delivery tasks...')
channel.start_consuming()
