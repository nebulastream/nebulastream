# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0], [7], [-42], [123456], [-1], [255]]


def udf(n):
    return "|".join([
        "%d" % n, "%5d" % n, "%-5d|" % n, "%05d" % n, "%+d" % n, "% d" % n, "%.3d" % n, "%07.3d" % n, "%-+6d|" % n,
        "%x" % n, "%#x" % n, "%X" % n, "%#06X" % n, "%o" % n, "%#o" % n, "%i" % n, "%u" % n,
        "%c" % (65 + (n % 26)), "%s" % n, "%r" % n, "%10s|" % n, "%-10s|" % n, "%%", "%5%",
        "a%db%sc" % (n, "x"), "%(a)s-%(b)s" % {"a": str(n), "b": "y"},
    ])
