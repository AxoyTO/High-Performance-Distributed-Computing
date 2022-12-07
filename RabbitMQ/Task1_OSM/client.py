# через клиент происходит взаимодействие с системой, запрос данных и получение ответа
# клиент отправляет букву, а получает список улиц, начинающихся на эту букву

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()  # создание канала
channel.queue_declare(queue="client-manager")  # создание очереди client-manager
channel.queue_declare(queue="manager-client")  # создание очереди manager-client

# Отправка и получение сообщений между клиентом и менеджером

while True:
    print(
        "Enter a single letter. To exit send EOF (Ctrl+D on Linux, Ctrl+Z on Windows)."
    )

    try:
        letter = input()
    except EOFError:
        print("EOF received from user. Exiting...")
        break

    if letter.isalpha() == False or len(letter) > 1:
        print("Please enter a single letter or send EOF to exit.")
        continue

    channel.basic_publish(
        exchange="", routing_key="client-manager", body=letter.lower()
    )  # отправка сообщения в очередь client-manager
    print(f" -> Client sent letter '{letter}' to manager.")

    requested_streets = []

    # Ожидание ответа от менеджера
    def callback(ch, method, properties, _body):
        if _body.decode("utf-8") == "quitflag":
            channel.stop_consuming()
        else:
            requested_streets.append(_body.decode("utf-8"))

    # Подписка на очередь manager-client и ожидание сообщений от менеджера в этой очереди (пока не получит quitflag)
    channel.basic_consume(
        queue="manager-client", on_message_callback=callback, auto_ack=True
    )
    channel.start_consuming()  # начинает ожидание сообщений

    if len(requested_streets):  # если список улиц не пустой
        print(requested_streets)
    else:
        print("There are no streets for this letter")


channel.basic_publish(
    exchange="", routing_key="client-manager", body="endflag"
)  # отправка сообщения в очередь client-manager о завершении работы клиента (после получения quitflag)

print("Client has stopped consuming from manager.")
connection.close()  # закрытие соединения
print("Client's connection with manager is closed.")
