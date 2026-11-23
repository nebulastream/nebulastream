# codon-fuzz: entry=case_entry args=INT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[662695200], [662688000], [670000000], [688000000], [694223999], [631152000], [946684800], [1262304000]]
# real-udf: origin=literature id=babelfish_arrow_timezone_year requires=arrow wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments: ts is a Babelfish Date with year() and asTs() (epoch seconds, UTC).

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/externalLibs/Queries.java#L148
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/externalLibs/Queries.java, lines 148-148
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

import site
import arrow as ar
def udf(rec,ctx):
	if(rec.ts.year() > 1990 and rec.ts.year() < 1992):
		pacfic = ar.get(rec.ts.asTs())
		pacfic = pacfic.to('US/Pacific')
		rs = pacfic.year
		return rs
	return 0

lambda rec,ctx: udf(rec,ctx)


import datetime


class BfDate:
    def __init__(self, seconds: int):
        self.seconds = seconds

    def year(self) -> int:
        return datetime.datetime.fromtimestamp(self.seconds, tz=datetime.timezone.utc).year

    def asTs(self) -> int:
        return self.seconds


class Rec:
    def __init__(self, ts: BfDate):
        self.ts = ts


def case_entry(ts):
    return udf(Rec(BfDate(ts)), None)
