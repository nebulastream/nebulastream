# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
def parse(tok):
    try:
        v = int(tok)
    except ValueError:
        try:
            return float(tok)
        except ValueError:
            return None
    else:
        return v * 1.0
    finally:
        pass

def udf(s, n):
    log = []
    items = s.split(",")
    for i in range(3):
        try:
            tok = items[n % 5 + i]
            v = parse(tok)
            if v is None:
                raise KeyError(tok)
            log.append(str(100 // int(v)))
        except IndexError:
            log.append("idx")
        except ZeroDivisionError:
            log.append("zero")
        except KeyError:
            log.append("key")
        except OverflowError:
            log.append("key")
        except Exception:
            log.append("exc")
        finally:
            log.append(".")
    return "".join(log)
