import re
from lpython import i64

def first_number(s: str) -> i64:
    match = re.search(r"\d+", s)
    return i64(int(match.group(0))) if match else i64(-1)

print(first_number("hello world 42"))
