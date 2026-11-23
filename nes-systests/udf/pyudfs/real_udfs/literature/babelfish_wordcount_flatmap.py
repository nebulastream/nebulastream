# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["regular deposits sleep carefully"], ["furiously ironic requests haggle quickly"], ["slyly final accounts boost. pending, regular foxes are about the"], ["blithely"], ["quickly even pinto beans wake  slyly"], ["express packages cajole furiously along the final, bold dependencies"]]
# real-udf: origin=literature id=babelfish_wordcount_flatmap requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments.
# Wrapper: the UDF emits each word through ctx; case_entry joins them with '|'.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/WordCount.java#L55
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/WordCount.java, lines 55-55
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

def wordcount(rec,ctx):
	words = rec.o_comment.split(" ")
	for w in words:
		ctx((w))
lambda rec,ctx:wordcount(rec,ctx)


class Rec:
    def __init__(self, o_comment: str):
        self.o_comment = o_comment


def case_entry(o_comment):
    emitted = []
    wordcount(Rec(o_comment), lambda word: emitted.append(word))
    return "|".join(emitted)
