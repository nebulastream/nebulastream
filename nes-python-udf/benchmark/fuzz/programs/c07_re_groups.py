# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["user=alice id=42 mail=Alice@Example.com"], ["key=  val=3"], ["ÄÖ=ü1 x=y"]]
import re

PAIR = re.compile(r"(?P<key>\w+)=(?P<val>\S*)")


def udf(s):
    out = []
    for m in PAIR.finditer(s):
        out.append(m.group("key") + ":" + m.group("val") + "@" + str(m.start()) + "-" + str(m.end()))
    m = re.search(r"(\w+)@(\w+)\.(com|org)", s, re.IGNORECASE)
    tail = [str(m.group(1)), str(m.group(2)), str(m.group(3))] if m else [""]
    return ";".join(out) + "|" + ",".join(tail) + "|" + str(len(PAIR.findall(s)))
