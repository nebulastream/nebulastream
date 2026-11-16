import array, sys, time
variant = sys.argv[1]
if variant == "thread":
    import threading
    t = threading.Thread(target=lambda: None); t.start(); t.join()
elif variant == "imports":
    import importlib, inspect, typing, collections, mmap, select, traceback, struct, os
elif variant == "socket":
    import socket
elif variant == "signal":
    import signal
    signal.signal(signal.SIGUSR1, lambda *a: None)
ns = {}
exec("def udf(x):\n    h = x\n    for _ in range(100):\n        h = (h * 1103515245 + 12345) % 2147483648\n    return h\n", ns)
udf = ns["udf"]
batch = array.array("q", range(2731))
for phase in range(2):
    t = time.time(); rows = 0
    while time.time() - t < 3:
        r = [udf(x) for x in batch]; rows += len(batch)
print("%-8s %.1f ns/row" % (variant, (time.time() - t) / rows * 1e9))
