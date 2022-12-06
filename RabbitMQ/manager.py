#!/usr/bin/env python3

import pika, sys

#STEP 0: get number of processes from starter script
number_of_processes = int(sys.argv[1])
dict = {l: (i % number_of_processes) for i, l in enumerate("abcdefghijklmnopqrstuvwxyz")} # d = {'а': 0, ... }



#STEP 1: receive message from client

#CLIENT CONNECTION
client_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
client_channel = client_connection.channel()
client_channel.queue_declare(queue='client-manager')
client_channel.queue_declare(queue='manager-client')

#EXHANGE WITH DIFFERENT PROCESSES
exchange_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
exchange_channel = exchange_connection.channel()
exchange_channel.exchange_declare(exchange='processes', exchange_type='direct')

#RECEIVE FROM ALL PROCESSES
process_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
process_channel = process_connection.channel()
process_channel.queue_declare(queue='processes-manager')


def callback_client(ch, method, properties, _body):
    print("MANAGER: message {} received from CLIENT".format(_body.decode("utf-8")))
    letter = _body.decode("utf-8")

    if (letter == "__end__"):
        client_channel.stop_consuming()
        for i in range(number_of_processes):
            exchange_channel.basic_publish(exchange='processes', routing_key=str(i), body=letter)
        return
    else:
        proc_number = dict[letter]
        print("MANAGER: sending message to {} PROCESS".format(proc_number))
        exchange_channel.basic_publish(exchange='processes', routing_key=str(proc_number), body=letter)



    #STEP 3: receive message from selected process and send it back to the client
    def callback_process(ch, method, properties, __body):
        street = __body.decode("utf-8")
        client_channel.basic_publish(exchange='', routing_key='manager-client', body=street)
        if (street == "__quit__"):
            process_channel.stop_consuming()


    process_channel.basic_consume(queue='processes-manager', on_message_callback=callback_process, auto_ack=True)
    process_channel.start_consuming()



client_channel.basic_consume(queue='client-manager', on_message_callback=callback_client, auto_ack=True)
client_channel.start_consuming()

print("MANAGER: consuming from client stopped")
client_connection.close()
exchange_connection.close()
process_connection.close()
print("MANAGER: connections closed")