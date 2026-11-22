# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED


def slugify(s):
    out = []
    prev_dash = False
    for ch in s.casefold():
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        elif not prev_dash:
            out.append("-")
            prev_dash = True
    return "".join(out).strip("-")


def udf(s):
    slug = slugify(s)
    return slug + "|" + str(len(slug)) + "|" + s.upper().lower() + "|" + s.swapcase().swapcase()
