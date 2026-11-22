# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    words = re.findall(r"\w+", s)
    digits = re.findall(r"\d", s)
    spaces = len(re.findall(r"\s", s))
    upper = re.sub(r"[^\W\d_]+", lambda m: m.group(0).upper(), s)
    ic = re.findall(r"(?i)straße|σ|i", s)
    return "|".join(words) + "#" + "".join(digits) + "#" + str(spaces) + "#" + upper + "#" + ",".join(ic)
