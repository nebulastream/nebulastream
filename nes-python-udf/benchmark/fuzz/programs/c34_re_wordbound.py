# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    a = re.sub(r"\b", "|", s[:15])
    b = re.findall(r"\b", s[:15])
    c = re.split(r"\b", s[:15])
    d = re.sub(r"(?<=\w)(?=\W)", "]", s[:15])
    return a + "#" + str(len(b)) + "#" + "/".join(str(x) for x in c) + "#" + d
