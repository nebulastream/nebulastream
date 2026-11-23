# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a"], ["abc"], [""], ["xa xx"], ["k=v; a=b"]]
# Before NES c20b3c9032+, the re plugin took an empty match twice where the previous empty match ended
# (re.sub(r"$", "<", "a") gave "a<<") and findall with one group gave whole matches; re.split did not compile in a join.
import re


def udf(s):
    return ("/".join(re.findall(r"x*", s)) + "|" + re.sub(r"x*", "-", s) + "|" + re.sub(r"$", "<", s) + "|"
            + ",".join(re.findall(r"(\w)=\w", s)) + "|" + "/".join(re.split(r"\s*", s)))
