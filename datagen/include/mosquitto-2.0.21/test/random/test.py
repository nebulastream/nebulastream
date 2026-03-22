#!/usr/bin/env python3

import subprocess
import time
import sys

def next_client(clients):
    if len(clients) == 0:
        return

    c = clients.pop()
    args = ["./random_client.py", str(c)]

    proc = subprocess.Popen(args, stderr=subprocess.DEVNULL)
    proc.cid = c
    return proc


def run_clients(max_clients):
    clients = list(range(1, max_clients))
    start_time = time.time()

    running_clients = []
    while True:
        print(len(running_clients))
        if len(running_clients) < max_clients:
            c = next_client(clients)
            if c is not None:
                running_clients.append(c)
        else:
            time.sleep(0.1)

        for c in running_clients:
            c.poll()
            if c.returncode is not None:
                running_clients.remove(c)
                clients.append(c.cid)
                c.terminate()
                c.wait()


env = {}
env["LD_LIBRARY_PATH"] = "../../lib"

#broker = subprocess.Popen(["../../src/mosquitto", "-c", "random.conf"], env=env)
run_clients(1000)
