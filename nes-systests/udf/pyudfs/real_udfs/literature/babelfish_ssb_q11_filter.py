# codon-fuzz: entry=case_entry args=INT64,INT64 returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[17, 2], [24, 1], [25, 2], [3, 3], [36, 2], [12, 0], [8, 4], [49, 10]]
# real-udf: origin=literature id=babelfish_ssb_q11_filter requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/tcph/queries/SSBQuery11.java#L99
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/tcph/queries/SSBQuery11.java, lines 99-99
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

udf = lambda rec,ctx: (rec.lo_quantity<25) and (rec.lo_discount >= 1) and (rec.lo_discount<=3)


class Rec:
    def __init__(self, lo_quantity: int, lo_discount: int):
        self.lo_quantity = lo_quantity
        self.lo_discount = lo_discount


def case_entry(lo_quantity, lo_discount):
    return udf(Rec(lo_quantity, lo_discount), None)
