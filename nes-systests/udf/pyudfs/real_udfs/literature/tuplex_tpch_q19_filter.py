# codon-fuzz: entry=case_entry args=VARSIZED,VARSIZED,INT64,INT64,VARSIZED,VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Brand#12", "SM BOX", 5, 3, "AIR", "DELIVER IN PERSON"], ["Brand#23", "MED PACK", 15, 8, "AIR REG", "DELIVER IN PERSON"], ["Brand#34", "LG CASE", 25, 12, "AIR", "DELIVER IN PERSON"], ["Brand#12", "SM BOX", 5, 3, "TRUCK", "DELIVER IN PERSON"], ["Brand#23", "MED PACK", 15, 8, "AIR REG", "TAKE BACK RETURN"], ["Brand#34", "LG CASE", 31, 12, "AIR", "DELIVER IN PERSON"], ["Brand#45", "JUMBO PKG", 20, 40, "MAIL", "NONE"], ["Brand#12", "MED BAG", 8, 4, "AIR", "DELIVER IN PERSON"]]
# real-udf: origin=literature id=tuplex_tpch_q19_filter requires= wrapped=1 ported=0
# Wrapper: Tuplex passes the row as a dict; case_entry builds it from the fields the UDF reads.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/tpch/Q19/runtuplex.py#L134-L157
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/tpch/Q19/runtuplex.py, lines 134-157

def Q19_filter_logical(x):
    return (
        (
            x["p_brand"] == "Brand#12"
            and x["p_container"] in ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
            and 1 <= x["l_quantity"] <= 1 + 10
            and 1 <= x["p_size"] <= 5
            and x["l_shipmode"] in ["AIR", "AIR REG"]
            and x["l_shipinstruct"] == "DELIVER IN PERSON")
        or (
            x["p_brand"] == "Brand#23"
            and x["p_container"] in ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
            and 10 <= x["l_quantity"] <= 10 + 10
            and 1 <= x["p_size"] <= 10
            and x["l_shipmode"] in ["AIR", "AIR REG"]
            and x["l_shipinstruct"] == "DELIVER IN PERSON"
        )
        or (
            x["p_brand"] == "Brand#34"
            and x["p_container"] in ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
            and 20 <= x["l_quantity"] <= 20 + 10
            and 1 <= x["p_size"] <= 15
            and x["l_shipmode"] in ["AIR", "AIR REG"]
            and x["l_shipinstruct"] == "DELIVER IN PERSON"))


def case_entry(p_brand, p_container, l_quantity, p_size, l_shipmode, l_shipinstruct):
    return Q19_filter_logical({"p_brand": p_brand, "p_container": p_container, "l_quantity": l_quantity,
                               "p_size": p_size, "l_shipmode": l_shipmode, "l_shipinstruct": l_shipinstruct})
