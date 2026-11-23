# codon-fuzz: entry=case_entry args=VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["JFK"], ["LAX"], ["ORD"], ["JFK"], ["ATL"], ["EWR"]]
# real-udf: origin=literature id=babelfish_maxdelay_filter requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments; JStr stands for Babelfish's Text with its equals() method.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/MaxDelay.java#L37
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/MaxDelay.java, lines 37-37
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

def udf1(rec,ctx):
	return rec.Origin.equals("JFK")
lambda rec,ctx: rec.Origin.equals("JFK")


class JStr:
    """Babelfish Text value: the Java-style string methods the UDF calls."""

    def __init__(self, value: str):
        self.value = value

    def equals(self, other: str) -> bool:
        return self.value == other


class Rec:
    def __init__(self, Origin: JStr):
        self.Origin = Origin


def case_entry(origin):
    return udf1(Rec(JStr(origin)), None)
