# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED


def udf(s, n):
    x = n / 7
    w = n % 12
    a = f"{s}|{s.upper()}|{n}|{s:>12}|{s:<12}|{s:^12}|{s:.2}" + s.ljust(w) + "|"
    b = s.ljust(8) + "|" + s.rjust(8)
    c = f"{s!r:>20}|{s[:3]!s:*^7}"
    d = f"{s[:2]}{n}{s[:2]}"
    e = f"{x:5.1f}%"
    return a + "#" + b + "#" + c + "#" + d + "#" + e
