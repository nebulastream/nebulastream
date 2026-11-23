# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["héllo"], ["a\\\\b"], ["€ x"], ["😀"], ["plain"]]


def udf(s):
    return ascii(s) + "|" + ascii((s, 1)) + "|" + "%a" % s
