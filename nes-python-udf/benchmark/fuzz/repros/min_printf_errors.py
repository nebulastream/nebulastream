# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a"], ["ab"]]


def attempt(f):
    try:
        return f()
    except TypeError as e:
        return "TypeError:" + str(e)
    except ValueError as e:
        return "ValueError:" + str(e)


def udf(s):
    return "|".join([
        attempt(lambda: "%d" % s), attempt(lambda: "%s %s" % (s,)), attempt(lambda: "%s" % (s, s)),
        attempt(lambda: "%c" % s), attempt(lambda: "%x" % 1.5), attempt(lambda: "%q" % s), attempt(lambda: "%" % s),
        attempt(lambda: "%f" % s),
    ])
