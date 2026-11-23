# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a,b,c"], ["Grüße"], [""]]
# Before Codon fork 3b8c3a9+, a variable named str, int or float was the class in attribute access (str.split(",")
# called str.split on ","), and f-strings called the user's variable named str or repr.


def udf(str):
    int = len(str)
    return "|".join(str.split(",")) + f"/{str}/{int}/{str!r}/" + repr(int.bit_length())
