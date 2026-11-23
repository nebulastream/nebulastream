# codon-fuzz: entry=case_entry args=BOOLEAN,FLOAT64,VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[false, 25.0, "AA"], [false, 10.0, "HA"], [false, 9.0, "AA"], [true, 45.0, "AA"], [false, 60.0, "DL"], [false, -4.0, "HA"], [false, 133.0, "UA"]]
# real-udf: origin=literature id=babelfish_etl_filter requires= wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments; JStr stands for Babelfish's Text with its equals() method.

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/ETLQuery.java#L68
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/analytics/queries/ETLQuery.java, lines 68-68
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

udf = lambda rec,ctx: (not rec.Cancelled) and (rec.DepDelay >=10) and (rec.IATA_CODE_Reporting_Airline.equals("AA") or rec.IATA_CODE_Reporting_Airline.equals("HA"))


class JStr:
    """Babelfish Text value: the Java-style string methods the UDF calls."""

    def __init__(self, value: str):
        self.value = value

    def equals(self, other: str) -> bool:
        return self.value == other


class Rec:
    def __init__(self, Cancelled: bool, DepDelay: float, IATA_CODE_Reporting_Airline: JStr):
        self.Cancelled = Cancelled
        self.DepDelay = DepDelay
        self.IATA_CODE_Reporting_Airline = IATA_CODE_Reporting_Airline


def case_entry(cancelled, dep_delay, airline):
    return udf(Rec(cancelled, dep_delay, JStr(airline)), None)
