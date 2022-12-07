# процессы-хранители данных
# принимают данные от менеджера и отправляют их в очередь

import pika, sys

proc_number = sys.argv[1]

streets = []

data_connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
data_channel = data_connection.channel()
data_channel.exchange_declare(exchange="ETL", exchange_type="direct")
result = data_channel.queue_declare(queue="", exclusive=False)
queue_name = result.method.queue
data_channel.queue_bind(exchange="ETL", queue=queue_name, routing_key=proc_number)


def callback_data(ch, method, properties, _body):
    if _body.decode("utf-8") == "quitflag":
        data_channel.stop_consuming()
    else:
        streets.append(_body.decode("utf-8"))


data_channel.basic_consume(
    queue=queue_name, on_message_callback=callback_data, auto_ack=True
)
data_channel.start_consuming()
data_connection.close()


receive_connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
receive_channel = receive_connection.channel()
receive_channel.exchange_declare(exchange="processes", exchange_type="direct")
result = receive_channel.queue_declare(queue="", exclusive=False)
queue_name = result.method.queue
receive_channel.queue_bind(
    exchange="processes", queue=queue_name, routing_key=proc_number
)


sending_connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
sending_channel = sending_connection.channel()
sending_channel.queue_declare(queue="processes-manager")


def callback(ch, method, properties, _body):
    print(f"Process {proc_number} received message from manager.")

    letter = _body.decode("utf-8")
    if letter == "endflag":
        receive_channel.stop_consuming()
    else:
        for street in streets:
            if street[0].lower() == letter:
                sending_channel.basic_publish(
                    exchange="", routing_key="processes-manager", body=street
                )

        sending_channel.basic_publish(
            exchange="", routing_key="processes-manager", body="quitflag"
        )
        print(f"Process {proc_number} sent message to manager.")


receive_channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True
)
receive_channel.start_consuming()


print(f"Process {proc_number} stopped consuming.")
receive_connection.close()
sending_connection.close()
print(f"Process {proc_number} closed connection.")
