# codon-fuzz: entry=case_entry args=FLOAT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[8.5], [-7.0], [20.0], [25.5], [30.0], [30.5], [104.0]]
# real-udf: origin=literature id=babelfish_etl_delay_class requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments.
# Wrapper: the UDF returns the record; case_entry returns the new delay field.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/ETLQuery.java#L85
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/ETLQuery.java, lines 85-85
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

def udf3(rec,ctx):
	if rec.avgDelay > 30:		rec.delay = "High"
	elif rec.avgDelay < 20:
		rec.delay = "Low"
	else:
		rec.delay = "Medium"
	return rec
lambda a,ctx: udf3(a,ctx)


class Rec:
    def __init__(self, avgDelay: float):
        self.avgDelay = avgDelay
        self.delay = ""


def case_entry(avg_delay):
    return udf3(Rec(avg_delay), None).delay
