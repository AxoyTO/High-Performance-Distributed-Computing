#!/usr/bin/env python3

import subprocess, pika, sys

#STEP 1: get data from osm-file

def check(street):
    if (street == ""):
        return False
    return street[0].isalpha()

message = subprocess.run("cat map.osm | grep \'addr:street\' | cut -d \'\"\' -f4", capture_output=True, text=True, shell=True)

streets = message.stdout
streets = streets.split('\n')

streets=list(set(streets))
streets = list(filter(check, streets))

print(streets)

#STEP 2: send data to processes

number_of_processes = int(sys.argv[1])
dict = {l: (i % number_of_processes) for i, l in enumerate("abcdefghijklmnopqrstuvwxyz")} # d = {'а': 0, ... }

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='ETL', exchange_type='direct')

for street in streets:
    channel.basic_publish(exchange='ETL', routing_key=str(dict[street[0].lower()]), body=street)

for i in range(number_of_processes):
    channel.basic_publish(exchange='ETL', routing_key=str(i), body="__quit__")

connection.close()
