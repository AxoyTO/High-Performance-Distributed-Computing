# знает, где какие данные хранятся

import pika, sys

# Получить количество процессов из аргументов командной строки
world_size = int(sys.argv[1])
dict = {
    l: (i % world_size) for i, l in enumerate("abcdefghijklmnopqrstuvwxyz")
}  # Словарь, где ключ - буква, значение - номер процесса, в котором хранится эта буква


# Соединение с клиентом и с другими процессами (процессы и клиент должны быть запущены)
# Соединение с клиентом
client_connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
client_channel = client_connection.channel()
client_channel.queue_declare(queue="client-manager")
client_channel.queue_declare(queue="manager-client")

# Соединение с процессами
exchange_connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
exchange_channel = exchange_connection.channel()
exchange_channel.exchange_declare(exchange="processes", exchange_type="direct")

# Соединение с менеджером процессов (для получения сообщений от процессов)
process_connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
process_channel = process_connection.channel()
process_channel.queue_declare(queue="processes-manager")


def callback_client(ch, method, properties, _body):
    print(f" -> Manager received letter '{_body.decode('utf-8')}' from client.")
    letter = _body.decode("utf-8")

    # Если клиент отправил EOF, то менеджер отправляет endflag всем процессам и завершает работу
    if letter == "endflag":
        client_channel.stop_consuming()
        for i in range(world_size):
            exchange_channel.basic_publish(
                exchange="processes", routing_key=str(i), body=letter
            )
        return
    else:  # Если клиент отправил букву, то менеджер отправляет ее процессу, в котором она хранится
        proc_number = dict[letter]
        print(f" -> Manager is sending message to process {proc_number}.")
        exchange_channel.basic_publish(
            exchange="processes", routing_key=str(proc_number), body=letter
        )  # Отправить букву процессу, в котором она хранится

    # Получить ответ от процесса и отправить его клиенту
    def callback_process(ch, method, properties, __body):
        street = __body.decode("utf-8")
        client_channel.basic_publish(
            exchange="", routing_key="manager-client", body=street
        )
        if street == "quitflag":
            process_channel.stop_consuming()

    process_channel.basic_consume(
        queue="processes-manager", on_message_callback=callback_process, auto_ack=True
    )
    process_channel.start_consuming()  # Здесь процесс блокируется, пока не получит ответ от процесса (вызов stop_consuming)


client_channel.basic_consume(
    queue="client-manager", on_message_callback=callback_client, auto_ack=True
)
client_channel.start_consuming()

print("Manager has stopped consuming from the client.")
client_connection.close()
exchange_connection.close()
process_connection.close()
print("Manager has closed all connections.")
