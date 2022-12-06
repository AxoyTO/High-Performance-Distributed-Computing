#!/usr/bin/env python3

import pika, sys

proc_number = sys.argv[1]

#STEP 0: receive data from ETL process

streets = []

data_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
data_channel = data_connection.channel()
data_channel.exchange_declare(exchange='ETL', exchange_type='direct')
result = data_channel.queue_declare(queue='', exclusive=False)
queue_name = result.method.queue
data_channel.queue_bind(exchange='ETL', queue=queue_name, routing_key=proc_number)

def callback_data(ch, method, properties, _body):
    if (_body.decode("utf-8") == "__quit__"):
        data_channel.stop_consuming()
    else:
#        print("street {} received".format(_body.decode("utf-8")))
        streets.append(_body.decode("utf-8"))


data_channel.basic_consume(queue=queue_name, on_message_callback=callback_data, auto_ack=True)
data_channel.start_consuming()
data_connection.close()

#streets = ['aaa', 'bbb', 'ccc', 'xxxxxx', 'yyyyyy', 'zzzzzzzzz']



#RECEIVING CONNECTION
receive_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
receive_channel = receive_connection.channel()
receive_channel.exchange_declare(exchange='processes', exchange_type='direct')
result = receive_channel.queue_declare(queue='', exclusive=False)
queue_name = result.method.queue
receive_channel.queue_bind(exchange='processes', queue=queue_name, routing_key=proc_number)


#SENDING CONNECTION
sending_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
sending_channel = sending_connection.channel()
sending_channel.queue_declare(queue='processes-manager')


def callback(ch, method, properties, _body):
    print("PROCESS {}: message received from MANAGER".format(proc_number))

    letter = _body.decode("utf-8")
    if (letter == "__end__"):
        receive_channel.stop_consuming()
    else:
        for street in streets:
            if (street[0].lower() == letter):
                sending_channel.basic_publish(exchange='', routing_key='processes-manager', body=street)

        sending_channel.basic_publish(exchange='', routing_key='processes-manager', body="__quit__")
        print("PROCESS {}: messages sent to MANAGER".format(proc_number))


receive_channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
receive_channel.start_consuming()


print("PROCESS {}: consuming stopped".format(proc_number))
receive_connection.close()
sending_connection.close()
print("PROCESS {}: connection closed".format(proc_number))