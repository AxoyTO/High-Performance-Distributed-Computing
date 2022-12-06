#!/usr/bin/env python3

import subprocess, sys

import time

number_of_proc = sys.argv[1]

for i in range(int(number_of_proc)):
    print("STARTER: {} process".format(i))
    subprocess.Popen(["python3", "process.py", str(i)], stdin=None, stdout=None, stderr=None, close_fds=True)


print("STARTER: ETL process")
subprocess.run(["python3", "ETL.py", number_of_proc])


print("STARTER: manager process")
subprocess.Popen(["python3", "manager.py", number_of_proc], stdin=None, stdout=None, stderr=None, close_fds=True)

print("STARTER: client process")
subprocess.run(["python3", "client.py"])

print("STARTER: end")