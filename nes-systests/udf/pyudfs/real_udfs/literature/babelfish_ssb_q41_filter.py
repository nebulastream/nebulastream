# codon-fuzz: entry=case_entry args=VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["MFGR#1"], ["MFGR#2"], ["MFGR#3"], ["MFGR#4"], ["MFGR#5"], ["MFGR#12"]]
# real-udf: origin=literature id=babelfish_ssb_q41_filter requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments; JStr stands for Babelfish's Text with its equals() method.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/tcph/queries/SSBQuery41.java#L123
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/tcph/queries/SSBQuery41.java, lines 123-123
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

udf = lambda rec,ctx: (rec.p_mfgr.equals("MFGR#1") or rec.p_mfgr.equals("MFGR#2"))


class JStr:
    """Babelfish Text value: the Java-style string methods the UDF calls."""

    def __init__(self, value: str):
        self.value = value

    def equals(self, other: str) -> bool:
        return self.value == other


class Rec:
    def __init__(self, p_mfgr: JStr):
        self.p_mfgr = p_mfgr


def case_entry(p_mfgr):
    return udf(Rec(JStr(p_mfgr)), None)
