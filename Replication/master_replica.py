# Все записи идут через главную реплику
# Репликация синхронная: главная
# реплика ожидает, чтобы новые значения записались на подчинённые

# Реплика (главная либо подчинённая), которая получила новое значение переменной, должна
# передать его дальше. Для этого, она случайным образом выбирает k соседей (k может быть
# прааметром или зашитым значением). Если кого-то из этих соседей нет нового значения
# данной переменной — передаёт ей обновление

# Для того, чтобы отличать новое/старое значение — необходимо присваивать значению номер
# версии (этим занимается главная реплика)

# После того как все реплики сообщат главной о том, что значение переменной обновлено,
# главная реплика распечатывает соответствующее сообщение.

import sys, time, pika, random

print(" * MASTER REPLICA STARTED")

data = {"x": "", "y": "", "z": ""}  # словарь с данными
timestamp = {"x": 0, "y": 0, "z": 0}  # словарь с временем изменения данных

print(f"Data: {data}")
print(f"Timestamps: {timestamp}")
print("")

id = 0  # id реплики
number_of_replicas = int(sys.argv[1])  # количество реплик

confirmations = 0  # количество подтверждений от реплик

seq = list(range(1, number_of_replicas + 1))  # последовательность реплик

k = number_of_replicas / 5  # k - количество соседей, которым отправляется сообщение
if k < 5:  # если k меньше 5, то
    k = number_of_replicas  # k = количеству реплик
    

# создаем соединение с сервером RabbitMQ
send_connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
send_channel = send_connection.channel()  # создаем канал для отправки сообщений


# создаем соединение с сервером RabbitMQ
client_connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
client_channel = client_connection.channel()  # создаем канал
# создаем очередь для общения с клиентом
client_channel.queue_declare(queue="client-master")


# создаем соединение с сервером RabbitMQ
receive_connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
receive_channel = receive_connection.channel()  # создаем канал для получения сообщений
receive_channel.queue_declare(queue="0")  # создаем очередь для общения с репликами

# функция для обработки сообщений от реплик
def callback_replicas(ch, method, properties, body):
    global confirmations  # используем глобальную переменную confirmations
    confirmations += (
        1  # увеличиваем количество подтверждений при получении сообщения от реплики
    )

    # если количество подтверждений равно количеству реплик, то значит все реплики обновили значение переменной
    if confirmations == number_of_replicas:
        confirmations = 0  # обнуляем количество подтверждений
        print(" * MASTER REPLICA: All replicas updated!")
        print("")
        receive_channel.stop_consuming()  # останавливаем получение сообщений от реплик


# функция для обработки сообщений от клиента
def callback_client(ch, method, properties, body):
    list = body.decode(
        "utf-8"
    ).split()  # декодируем сообщение от клиента и разбиваем на слова
    print(
        f" * MASTER REPLICA: Received message {list} from client"
    )  # выводим сообщение от клиента

    key = list[0]  # ключ переменной
    value = ""  # строка, в которую будем записывать значение переменной
    for i in range(1, len(list)):
        value += list[i]
        value += " "
    value = value[:-1]

    if key not in data:
        print("Enter 'x', 'y' or 'z' first. Try again.")
        return

    data[key] = value
    timestamp[key] += 1

    print(f"Data: {data}")
    print(f"Timestamps: {timestamp}")

    random_seq = random.sample(
        seq, k
    )  # выбираем k реплик, которым отправляем сообщение

    # отправляем сообщение в очередь каждой реплики
    for (
        destination
    ) in random_seq:  # destination - номер очереди, в которую отправляем сообщение
        send_channel.queue_declare(
            queue=str(destination)
        )  # создаем очередь для отправки сообщения
        send_channel.basic_publish(
            exchange="",
            routing_key=str(destination),
            body=key
            + " "
            + value
            + " "
            + str(timestamp[key]),  # отправляем ключ, значение и timestamp
        )

    # если количество реплик больше 0, то начинаем получать сообщения от реплик
    if number_of_replicas > 0:
        receive_channel.basic_consume(
            queue="0", on_message_callback=callback_replicas, auto_ack=True
        )
        receive_channel.start_consuming()  # начинаем получать сообщения от реплик


client_channel.basic_consume(
    queue="client-master", on_message_callback=callback_client, auto_ack=True
)

try:
    # начинаем получать сообщения от клиента
    client_channel.start_consuming()
except EOFError as e:
    print("MASTER-REPLICA: Received Ctrl+D, exiting...")

send_connection.close()
receive_connection.close()
client_connection.close()

print("MASTER-REPLICA: All connections closed!")
time.sleep(5)
