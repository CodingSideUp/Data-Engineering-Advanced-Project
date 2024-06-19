import pika

# Connect to the local RabbitMQ instance
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue (to ensure it exists)
channel.queue_declare(queue='hello')

# Define a callback function to handle messages
def callback(ch, method, properties, body):
    print(f" [x] Received {body}")

# Subscribing to the queue
channel.basic_consume(queue='hello',
                      on_message_callback=callback,
                      auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
