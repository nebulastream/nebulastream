# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], [""], ["héllo"], ["x"], ["it's"]]


def udf(s):
    return "|".join([
        "%s" % s, "%r" % s, "%8s" % s, "%-8s|" % s, "%.2s" % s, "%8.2s" % s, "[%s]" % (s,), "%s=%s" % (s, len(s)),
        "%s/%d" % (s, len(s)), "%(k)r" % {"k": s}, "%a" % s, "no args" % (), "%s" % [s, s], "%s" % (s, )
    ])
