# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
def lookup(d, lst, key, idx):
    try:
        return d[key] + lst[idx]
    except LookupError:
        return -1

def safe_div(a, b):
    try:
        return a // b
    except ArithmeticError:
        return -2

def conv(s):
    try:
        return int(s)
    except Exception:
        return -3

def udf(s, n):
    d = {"a": 1, "b": 2}
    lst = [10, 20, 30]
    key = s[:1].lower()
    r1 = lookup(d, lst, key, n % 7 - 3)
    r2 = safe_div(n, n % 3)
    r3 = conv(s)
    return f"{r1},{r2},{r3}"
