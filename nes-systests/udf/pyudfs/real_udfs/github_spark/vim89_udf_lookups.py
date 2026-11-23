# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["payment_mode", "CC;DC;UPI"], ["payment_mode", "CASH"], ["country", "IN;US;XX"], ["country", ""], ["status", "A;I;P;Z"]]
# real-udf: origin=github-spark id=vim89_udf_lookups requires= wrapped=1 ported=0
# Wrapper: upstream fills the global lookup dict ('<column> <code>' -> description) from a lookup table
# Wrapper: at runtime; the case fills it with fixed entries.
# Source: https://github.com/vim89/datapipelines-essentials-python/blob/1bcae0d7b6c56e49dcc002ad261297819d6d1b66/src/com/vitthalmirji/etl/ETL.py#L56-L69
# Repository: vim89/datapipelines-essentials-python @ commit 1bcae0d7b6c56e49dcc002ad261297819d6d1b66
# License: Apache-2.0
# Copied verbatim from src/com/vitthalmirji/etl/ETL.py (L8-L8, L56-L69); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @f.udf(returnType=StringType())

lookup = {}


def udfLookups(clname, s):
    finallookupvalue = []
    if s is None:
        return ""
    else:
        codes = str(s).split(sep=';')
        for cd in codes:
            if f"{clname} {cd}" in lookup.keys():
                finallookupvalue.append(lookup[f"{clname} {cd}"])
            else:
                finallookupvalue.append(cd)

    return ';'.join(finallookupvalue)


lookup.update({
    "payment_mode CC": "Credit Card",
    "payment_mode DC": "Debit Card",
    "payment_mode UPI": "Unified Payments Interface",
    "payment_mode CASH": "Cash",
    "country IN": "India",
    "country US": "United States",
    "status A": "Active",
    "status I": "Inactive",
    "status P": "Pending",
})


def case_entry(clname, s):
    return udfLookups(clname, s)
