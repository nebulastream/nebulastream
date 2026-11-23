# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["abc"], [""], ["none"], ["k"]]
# Before Codon fork 3b8c3a9+, `x or default` and `x and y` with an Optional x built a Union with an Optional member:
# methods of the result were not found ("invalid union call") and values were read wrongly; `d.get(k) or "-"` did not
# compile, and the compiler crashed when it removed the IR types of such a UDF.


def udf(s):
    x = None if s == "none" else s
    n = None if s == "none" else len(s)
    d = {"k": "K"}
    return (x or "dflt").upper() + "|" + str((n or 7) + 1) + "|" + str(x and len(x)) + "|" + (d.get(s) or "-")
