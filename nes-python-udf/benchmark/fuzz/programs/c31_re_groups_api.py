# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["2020-01-31 err=5"], ["k1=v1, k2=v2"], ["no match here"]]
import re

DATE = re.compile(r"(?P<y>\d{4})-(?P<m>\d\d)-(?P<d>\d\d)")


def udf(s):
    m = DATE.search(s)
    if m is None:
        return "none|" + str(len(re.findall(r"(\w+)=(\w+)", s)))
    gd = m.groupdict()
    return m.group(0) + "|" + gd["y"] + gd["m"] + "|" + str(m.lastindex) + "|" + str(m.lastgroup) + "|" + m.expand(r"\g<d>/\g<m>")
