# codon-fuzz: entry=udf args=FLOAT64 returns=FLOAT64
STATE = [0.0, 0.0, 0.0]
COUNTS: dict[int, int] = {}

def udf(x):
    if x != x or abs(x) > 1e150:
        return -1.0
    STATE[0] += 1
    d = x - STATE[1]
    STATE[1] += d / STATE[0]
    STATE[2] += d * (x - STATE[1])
    b = int(x // 10) if abs(x) < 1e15 else 0
    COUNTS[b] = COUNTS.get(b, 0) + 1
    return STATE[2] / STATE[0] + COUNTS[b]
