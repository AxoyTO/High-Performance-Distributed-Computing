import pika

# establish connection with RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# create a "hello" queue to which the msg will be delivered
channel.queue_declare(queue='hello')

# empty string in exchange means default exchange
# routing_key is the queue name
channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')
print(" [x] Sent 'Hello World...'")

# make sure the network buffers were flushed and our message was actually delivered to RabbitMQ
connection.close()
