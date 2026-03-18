import sys
print("hello")
for line in sys.stdin:
    print(f"received: {line.strip()}")