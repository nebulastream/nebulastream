# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED


SEP = " "
EMO = "\uD7FF"
OCT = "\101\0x"
BYTES_LIKE = "\x7f\xff\t\v\a"


def udf(s):
    parts = [SEP, EMO, OCT, BYTES_LIKE, '\'' + "\""]
    return s + "|" + "|".join(str(len(p)) + ":" + ",".join(hex(ord(c)) for c in p) for p in parts)
