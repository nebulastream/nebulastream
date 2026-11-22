# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["price: 12,50 EUR; qty=３; tax=1e3"], ["-0.5 +.5 5. 0x10 1_0 ٣"], ["a=1;b=2.5;c=x"]]


def to_num(tok):
    try:
        return str(int(tok))
    except ValueError:
        pass
    try:
        return repr(float(tok))
    except ValueError:
        return "?"


def udf(s):
    toks = s.replace(";", " ").replace("=", " ").replace(",", ".").split()
    return " ".join(to_num(t) for t in toks)
