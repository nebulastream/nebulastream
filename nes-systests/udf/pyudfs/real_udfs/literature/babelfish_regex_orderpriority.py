# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1-URGENT"], ["2-HIGH"], ["3-MEDIUM"], ["4-NOT SPECIFIED"], ["5-LOW"], ["4-NOT SPECIFIE"]]
# real-udf: origin=literature id=babelfish_regex_orderpriority requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments; JStr stands for Babelfish's Text with its asString() method.
# Wrapper: the UDF returns the record; case_entry returns the rewritten o_orderpriority field.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/externalLibs/Queries.java#L39
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/externalLibs/Queries.java, lines 39-39
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

import re
pattern = re.compile('NOT SPECIFIED*')
def udf(rec,ctx):
	rs = re.sub(pattern,'none', rec.o_orderpriority.asString())
	rec.o_orderpriority = rs
	return rec

lambda rec,ctx: udf(rec,ctx)


class JStr:
    """Babelfish Text value: the Java-style string method the UDF calls."""

    def __init__(self, value: str):
        self.value = value

    def asString(self) -> str:
        return self.value


class Rec:
    def __init__(self, o_orderpriority: JStr):
        self.o_orderpriority = o_orderpriority


def case_entry(o_orderpriority):
    return udf(Rec(JStr(o_orderpriority)), None).o_orderpriority
