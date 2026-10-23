import pika
import json
import time

def callback(ch, method, properties, body):
    order = json.loads(body)
    print(f"[ChefService] Cooking started for Order: {order['order_id']}")
    time.sleep(3)  # Simulate cooking
    print(f"[ChefService] Cooking done for Order: {order['order_id']}")

    # Send to notification queue
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='notification_queue')
    channel.basic_publish(
        exchange='',
        routing_key='notification_queue',
        body=json.dumps(order)
    )
    connection.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='chef_queue')
channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue='chef_queue', on_message_callback=callback)
print('[ChefService] Waiting for orders...')
channel.start_consuming()
