# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1-URGENT"], ["2-HIGH"], ["3-MEDIUM"], ["4-NOT SPECIFIED"], ["5-LOW"], ["3-Medium"]]
# real-udf: origin=literature id=babelfish_string_reverse requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments.
# Wrapper: the UDF returns the record; case_entry returns the rewritten o_orderpriority field.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/string/queries/StringReverseNaive.java#L37
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/string/queries/StringReverseNaive.java, lines 37-37
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

def udf(rec,ctx):
	rec.o_orderpriority = rec.o_orderpriority[::-1]
	return rec
lambda a,ctx: udf(a,ctx)


class Rec:
    def __init__(self, o_orderpriority: str):
        self.o_orderpriority = o_orderpriority


def case_entry(o_orderpriority):
    return udf(Rec(o_orderpriority), None).o_orderpriority
