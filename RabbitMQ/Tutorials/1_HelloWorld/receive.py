import pika
import sys
import os


def main():
    # establish connection with RabbitMQ server
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # we could avoid declaring the queue since we know it already exists
    # it's a good practice to repeat declaring the queue in both programs
    channel.queue_declare(queue='hello')

    # whenever we receive a message, this callback function is called by the Pika library

    def callback_func(ch, method, properties, body):
        print(" [x] Received %r" % body)

    # we need to tell RabbitMQ that this particular callback function should receive messages from our hello queue
    channel.basic_consume(queue='hello',
                          auto_ack=True,
                          on_message_callback=callback_func)

    # we enter a never-ending loop that waits for data and runs callbacks whenever necessary,
    # and catch KeyboardInterrupt during program shutdown
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
