# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
# codon-fuzz-inputs: [["a\ud83d\ude00b\u0301c", 1], ["日本語テキスト", 3], ["e\u0301\u0302", 2]]


def udf(s, n):
    if not s:
        return ""
    k = n % len(s)
    parts = [s[k], s[-k - 1], s[k:k + 2], s[::2], str(len(s)), str(ord(s[k])), chr(ord(s[k]) + 1 if ord(s[k]) < 0x10FFFF else 65)]
    codes = ",".join(hex(ord(c)) for c in s[:5])
    return "/".join(parts) + ":" + codes
