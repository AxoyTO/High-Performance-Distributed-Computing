# Реализовать систему хранения данных с репликацией. Тип репликации: главный-
# подчинённые. Все записи идут через главную реплику. Репликация синхронная: главная
# реплика ожидает, чтобы новые значения записались на подчинённые.

import subprocess, sys

number_of_replicas = sys.argv[1] if len(sys.argv) > 1 else 2 

# запускаем главную реплику
subprocess.Popen([sys.executable, "master_replica.py", str(number_of_replicas)])

# запускаем подчиненные реплики
for i in range(int(number_of_replicas)):
    print(f"Replica {i+1} started.")
    subprocess.Popen([sys.executable,"replica.py",str(i + 1),str(number_of_replicas)])
