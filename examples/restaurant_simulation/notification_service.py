import pika
import json
import time
import random

def callback(ch, method, properties, body):
    order = json.loads(body)
    print(f"[NotificationService] Notifying customer for Order: {order['order_id']}")
    time.sleep(2)

    # Retry mechanism
    if random.random() < 0.3:  # 30% chance of failure
        print("[NotificationService] Customer unavailable, retrying...")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    print(f"[NotificationService] Customer notified for Order: {order['order_id']}")

    # Send to delivery queue
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='delivery_queue')
    channel.basic_publish(
        exchange='',
        routing_key='delivery_queue',
        body=json.dumps(order)
    )
    connection.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='notification_queue')
channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue='notification_queue', on_message_callback=callback)
print('[NotificationService] Waiting for notifications...')
channel.start_consuming()
