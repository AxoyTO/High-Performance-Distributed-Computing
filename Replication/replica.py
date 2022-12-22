# На каждой реплике хранятся три ключа — x, y, z. Их значения — строки
# Каждая реплика знает, как связаться со всеми остальными репликами

# Реплика (главная либо подчинённая), которая получила новое значение переменной, должна
# передать его дальше. Для этого, она случайным образом выбирает k соседей (k может быть
# прааметром или зашитым значением). Если кого-то из этих соседей нет нового значения
# данной переменной — передаёт ей обновление

# Каждая реплика, которая получила новое значение:
# • сообщает об этом главной реплике
# • распечатывает новое значение своей копии переменной

import sys, pika, time
import random

id = sys.argv[1]  # id реплики
number_of_replicas = int(sys.argv[2])  # количество реплик
print(f" ** REPLICA {id} STARTED")

data = {"x": "", "y": "", "z": ""}
timestamp = {"x": 0, "y": 0, "z": 0}
print(f"Data: {data}")  # данные реплики
print(f"Timestamps: {timestamp}")  # время последнего обновления данных
print("")

seq = list(range(0, number_of_replicas + 1))  # список реплик
seq.remove(int(id))  # удаляем из списка себя

k = number_of_replicas / 5  # k - количество соседей
if k < 5: # если k меньше 5, то
    k = number_of_replicas # k = количеству реплик

# создаем соединение для получения сообщений от других реплик
receive_connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
receive_channel = receive_connection.channel()  # канал для получения сообщений
receive_channel.queue_declare(queue=id)  # создаем очередь с именем id реплики


# создаем соединение для отправки сообщений другим репликам (главной и подчиненным)
send_connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
send_channel = send_connection.channel()  # канал для отправки сообщений

# функция, которая вызывается при получении сообщения
def receive_callback(ch, method, properties, body):
    list = body.decode("utf-8").split()  # разбиваем сообщение на слова
    key = list[0]  # первое слово - имя переменной
    # print(f" ** REPLICA {id}: Received message {body.decode('utf-8')}")

    value = ""
    for i in range(1, len(list) - 1):  # остальные слова - значение переменной
        value += list[i]
        value += " "
    value = value[:-1]

    time_stamp = int(list[len(list) - 1])  # последнее слово - время обновления

    # если время обновления больше, чем у нас, то обновляем данные
    if timestamp[key] < time_stamp:

        data[key] = value  # обновляем данные
        timestamp[key] = time_stamp  # обновляем время обновления

        print(f" ** REPLICA {id}: Received message {body.decode('utf-8')}")
        print(f"Data: {data}")
        print(f"Timestamps: {timestamp}")

        random_seq = random.sample(
            seq, k
        )  # выбираем реплики, которым отправляем обновление
        for destination in random_seq:  # отправляем обновление каждой реплике
            send_channel.queue_declare(
                queue=str(destination)
            )  # создаем очередь с именем id реплики
            # отправляем сообщение в очередь с именем id реплики в формате "имя переменной значение переменной время обновления"
            send_channel.basic_publish(
                exchange="",
                routing_key=str(destination),
                body=key + " " + value + " " + str(time_stamp),
            )

        # отправляем сообщение в очередь с именем 0, чтобы главная реплика обновила данные
        send_channel.queue_declare(queue="0")  # создаем очередь с именем 0
        send_channel.basic_publish(
            exchange="", routing_key="0", body="confirm"
        )  # отправляем сообщение в очередь с именем 0


receive_channel.basic_consume(
    queue=id, on_message_callback=receive_callback, auto_ack=True
)  # подписываемся на очередь с именем id реплики
try:
    receive_channel.start_consuming()  # начинаем получать сообщения
except EOFError as e:
    print(
        " ** REPLICA {id} IS STOPPED."
    )  # если нажали Ctrl+C, то прекращаем получать сообщения

receive_connection.close()  # закрываем соединение для получения сообщений
send_connection.close()  # закрываем соединение для отправки сообщений

time.sleep(5)
