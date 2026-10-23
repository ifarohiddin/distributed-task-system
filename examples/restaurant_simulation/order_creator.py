import pika
import json
import uuid

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='chef_queue')

order = {
    'order_id': str(uuid.uuid4()),
    'items': ['Pizza', 'Coke']
}

channel.basic_publish(
    exchange='',
    routing_key='chef_queue',
    body=json.dumps(order)
)

print(f"[OrderCreator] Order sent to chef: {order['order_id']}")
connection.close()
