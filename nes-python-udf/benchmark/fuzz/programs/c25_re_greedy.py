# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["<a><b>text</b></a>"], ["\"x\" and \"y\""], ["aaaa"]]
import re


def udf(s):
    a = re.findall(r"<.+?>", s)
    b = re.findall(r"\"(.*?)\"", s)
    c = re.findall(r"a{2,3}?", s)
    d = re.findall(r"(?:ab)+|a", s)
    e = re.match(r"(a+)(a*)", s)
    tail = e.group(1) + "/" + e.group(2) if e else "-"
    return ",".join(a) + "|" + ",".join(b) + "|" + ",".join(c) + "|" + ",".join(d) + "|" + tail
