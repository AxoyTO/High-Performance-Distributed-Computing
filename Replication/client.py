# Клиент принимает команды вида: <имя переменной>, <новое значение>. Отсылает их
# главной реплике. Далее обновление распространяется с помощью эпидемического протокола (gossip algorithm)

import pika

# Подключаемся к главной реплике
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel() # Создаем канал
channel.queue_declare(queue="client-master") # Создаем очередь для клиента и главной репликой


while True:
    print(" ** CLIENT: Enter a command in the format:")
    print(" - 'x', 'y', or 'z' as key at first place with a space afterwards.")
    print(" - Then enter your message.")
    print(" - Ctrl+D to exit):")
    print(" - Example: 'x Hello world!' (without quotes)")
    print("Now enter your command: ", end="")

    try:
        string = input() # Читаем команду
        list = string.split() # Разбиваем на слова
    except EOFError as e: # Если Ctrl+D, то выходим
        break

    # Отправляем команду главной реплике
    channel.basic_publish(exchange="", routing_key="client-master", body=string)
    print(f"CLIENT: Message {string} sent to MASTER-REPLICA")


connection.close() # Закрываем соединение
print("CLIENT: Connection with master closed")
