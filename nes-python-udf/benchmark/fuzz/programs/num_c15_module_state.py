# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
SEEN: dict[int, int] = {}
HIST: list[int] = []
COUNT = 0
TOTAL = [0.0]

def udf(n):
    global COUNT
    COUNT += 1
    b = n % 5
    SEEN[b] = SEEN.get(b, 0) + 1
    HIST.append(b)
    if len(HIST) > 4:
        HIST.pop(0)
    TOTAL[0] += n / 1e6
    return f"{COUNT}:{SEEN[b]}:{HIST}:{round(TOTAL[0], 3)}"
