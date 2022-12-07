# starter.py запускает все процессы системы
import subprocess, sys

world_size = sys.argv[1]  # получаем количество процессов из аргументов командной строки

for i in range(int(world_size)):
    subprocess.Popen([sys.executable, "process.py", str(i)])  # запускаем процессы
    print(f" * Process {i} started.")


print(" * Starting ETL process...")
subprocess.run([sys.executable, "ETL.py", world_size])  # запускаем ETL


print(" * Starting manager process...")
subprocess.Popen([sys.executable, "manager.py", world_size])  # запускаем менеджер

print(" * Starting client process...")
subprocess.run([sys.executable, "client.py"])  # запускаем клиент

print("  Ending...")
