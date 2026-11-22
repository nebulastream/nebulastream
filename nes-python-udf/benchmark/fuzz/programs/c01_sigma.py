# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["ΟΔΟΣ ΚΑΙ ΣΟΦΙΑ"], ["ΑΣ."], ["Σ"], ["ΣΑΣ ΣΑΣ"], ["İI"], ["ǅungla ǆ"], ["ᾈᾼ"]]


def normalize_name(word):
    low = word.lower()
    return low[:1].upper() + low[1:]


def udf(s):
    words = s.split()
    out = [normalize_name(w) for w in words]
    return "|".join(out) + "#" + s.lower() + "#" + s.casefold() + "#" + s.title()
