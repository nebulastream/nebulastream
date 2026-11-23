# codon-fuzz: entry=case_entry args=VARSIZED,FLOAT64,INT64 returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1994-03-15", 0.06, 17], ["1994-11-02", 0.07, 5], ["1993-12-30", 0.06, 10], ["1995-01-01", 0.06, 3], ["1994-06-21", 0.05, 20], ["1994-08-09", 0.06, 24], ["1994-01-01", 0.06, 23], ["1996-02-14", 0.02, 41]]
# real-udf: origin=literature id=babelfish_tpch_q6_filter requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments; BfDate stands for Babelfish's Date with strict after()/before() on ISO dates.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/parallel/queries/Query6.java#L104
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/parallel/queries/Query6.java, lines 104-104
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

udf = lambda rec,ctx:  rec.l_shipdate.after("1994-01-01") & rec.l_shipdate.before("1995-01-01") and (rec.l_discount > 0.05) & (rec.l_discount < 0.07) & (rec.l_quantity < 24)


class BfDate:
    def __init__(self, value: str):
        self.value = value

    def after(self, other: str) -> bool:
        return self.value > other

    def before(self, other: str) -> bool:
        return self.value < other


class Rec:
    def __init__(self, l_shipdate: BfDate, l_discount: float, l_quantity: int):
        self.l_shipdate = l_shipdate
        self.l_discount = l_discount
        self.l_quantity = l_quantity


def case_entry(l_shipdate, l_discount, l_quantity):
    return udf(Rec(BfDate(l_shipdate), l_discount, l_quantity), None)
