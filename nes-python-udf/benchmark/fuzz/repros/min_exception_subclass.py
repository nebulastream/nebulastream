# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a"]]
class MyError(ValueError):
    pass


def udf(s):
    try:
        raise MyError("bad " + s)
    except MyError as e:
        return "caught:" + str(e)
