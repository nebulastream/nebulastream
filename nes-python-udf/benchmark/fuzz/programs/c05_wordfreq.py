# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED


def udf(s):
    counts = {}
    for w in s.lower().split():
        w = w.strip(".,;:!?\"'()[]{}")
        if w:
            counts[w] = counts.get(w, 0) + 1
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    uniq = set(counts)
    first = list(counts)
    return ";".join(f"{k}={v}" for k, v in ordered) + "|" + ",".join(sorted(uniq)) + "|" + ",".join(first)
