def printf(n: int) -> str:
    return "%05d|%s" % (n, "x")


def fstring(n: int) -> str:
    return f"{n:05d}|{'x'}"
