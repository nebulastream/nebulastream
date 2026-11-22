# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
class BadToken(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)

def check(tok):
    if not tok:
        raise BadToken("empty")
    if tok[0].isdigit():
        raise ValueError("digit")
    if len(tok) > 5:
        raise KeyError(tok)
    return tok.upper()

def udf(s):
    out = []
    errs = 0
    for tok in s.split(","):
        try:
            out.append(check(tok))
        except BadToken:
            out.append("<b>")
        except ValueError:
            out.append("<v>")
            errs += 1
        except KeyError as e:
            out.append("<k>")
            if errs > 1:
                break
    return "".join(out) + str(errs)
