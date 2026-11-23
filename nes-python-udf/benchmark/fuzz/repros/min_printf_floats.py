# codon-fuzz: entry=udf args=FLOAT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0.0], [3.14159], [-2.5], [1e-7], [123456789.125], [1e300], [-0.0]]


def udf(x):
    return "|".join([
        "%f" % x, "%.2f" % x, "%10.3f" % x, "%-10.1f|" % x, "%+.1f" % x, "%010.2f" % x, "% .3f" % x,
        "%e" % x, "%.3E" % x, "%g" % x, "%G" % x, "%.10g" % x, "%#g" % x, "%#.0f" % x, "%F" % x,
        "%d" % x, "%s" % x, "%r" % x, "%.3s" % x, "%5.1f%%" % (x,),
    ])
