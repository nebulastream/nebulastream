# codon-fuzz: entry=case_entry args=FLOAT64,FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[12.0, 5.0], [-3.0, -11.0], [45.0, 52.0], [0.0, 0.0], [7.0, -2.0], [120.0, 98.0]]
# real-udf: origin=literature id=babelfish_etl_avg_delay requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments.
# Wrapper: the UDF returns the record; case_entry returns the new avgDelay field.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/ETLQuery.java#L80
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/ETLQuery.java, lines 80-80
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

def udf2(rec,ctx):
	rec.avgDelay = (rec.DepDelay + rec.ArrDelay) / 2 
	return rec
lambda a,ctx: udf2(a,ctx)


class Rec:
    def __init__(self, DepDelay: float, ArrDelay: float):
        self.DepDelay = DepDelay
        self.ArrDelay = ArrDelay
        self.avgDelay = 0.0


def case_entry(dep_delay, arr_delay):
    return udf2(Rec(dep_delay, arr_delay), None).avgDelay
