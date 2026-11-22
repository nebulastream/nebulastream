# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
def udf(s, n):
    words = s.split()
    rec = [(w, len(w), i) for i, w in enumerate(words)]
    by_len = sorted(rec, key=lambda r: r[1])
    by_len_rev = sorted(rec, key=lambda r: r[1], reverse=True)
    lst = list(s)
    lst.sort(key=str.lower)
    lst2 = sorted(s, key=lambda c: (c.isdigit(), c.lower()), reverse=True)
    nums = [(n >> k) % 5 for k in range(8)]
    nums.sort(reverse=True)
    return str([r[2] for r in by_len]) + str([r[2] for r in by_len_rev]) + "".join(lst) + "".join(lst2) + str(nums)
