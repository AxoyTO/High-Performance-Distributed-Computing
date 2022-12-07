# ETL - Extract, Transform, Load
# загрузчик данных
import subprocess, pika, sys

# Проверка на пустую строку и на то, что строка начинается с буквы
def is_valid(street):
    if street == "":
        return False
    return street[0].isalpha()


# Extract - Получаем данные из файла map.osm
message = subprocess.run(
    "cat map.osm | grep 'addr:street' | cut -d '\"' -f4",
    capture_output=True,
    text=True,
    shell=True,
)  # message.stdout - содержит все улицы из файла map.osm

streets = message.stdout  # получаем все улицы из файла map.osm
streets = streets.split("\n")  # разбиваем на строки

streets = list(set(streets))  # убираем дубликаты
streets = list(
    filter(is_valid, streets)
)  # убираем пустые строки и строки, которые не начинаются с буквы

for i in streets:
    print(i, end=" - " if i != streets[-1] else "\n")  # выводим все улицы

world_size = int(
    sys.argv[1]
)  # получаем количество процессов из аргументов командной строки
dict = {
    l: (i % world_size) for i, l in enumerate("abcdefghijklmnopqrstuvwxyz")
}  # создаем словарь, в котором ключ - буква, значение - номер процесса

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)  # создаем соединение с сервером RabbitMQ
channel = connection.channel()  # создаем канал
channel.exchange_declare(exchange="ETL", exchange_type="direct")  # объявляем exchange

for street in streets:
    channel.basic_publish(
        exchange="ETL", routing_key=str(dict[street[0].lower()]), body=street
    )  # отправляем улицы в очередь


for i in range(world_size):
    channel.basic_publish(
        exchange="ETL", routing_key=str(i), body="quitflag"
    )  # отправляем сообщение о завершении - quitflag

connection.close()  # закрываем соединение
