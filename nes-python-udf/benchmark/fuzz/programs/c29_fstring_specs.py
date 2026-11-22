# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED


def udf(s, n):
    name = s[:6]
    a = f"{name:>8}|{name:<8}|{name:^8}|{name:_^9}|{name:.2}|{name:8.3}"
    b = f"{name!r}|{name!s:>4}"
    c = chr(65 + n % 26)
    d = f"{n:,}|{n:#x}|{n:#o}"
    return a + "#" + b + "#" + c + "#" + d
