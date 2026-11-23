# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["regular deposits sleep carefully"], ["furiously ironic requests haggle quickly"], ["slyly final accounts boost. pending, regular foxes are about the"], ["blithely"], ["quickly even pinto beans wake  slyly"], ["express packages cajole furiously along the final, bold dependencies"]]
# real-udf: origin=literature id=babelfish_twogram_flatmap requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments; JStr stands for Babelfish's Text with split() and concat().
# Wrapper: the UDF emits each two-gram through ctx; case_entry joins them with '|'.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/TwoGram.java#L56
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/TwoGram.java, lines 56-56
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

def wordcount(rec,ctx):
	words = rec.o_comment.split(" ")
	length = len(words)
	index = 1
	while index < length:
		twogram = words[index-1].concat(words[index])
		index = index +1
		ctx(twogram)
lambda rec,ctx:wordcount(rec,ctx)


class JStr:
    """Babelfish Text value: the Java-style string methods the UDF calls."""

    def __init__(self, value: str):
        self.value = value

    def split(self, separator: str):
        return [JStr(part) for part in self.value.split(separator)]

    def concat(self, other):
        return JStr(self.value + other.value)


class Rec:
    def __init__(self, o_comment: JStr):
        self.o_comment = o_comment


def case_entry(o_comment):
    emitted = []
    wordcount(Rec(JStr(o_comment)), lambda twogram: emitted.append(twogram.value))
    return "|".join(emitted)
