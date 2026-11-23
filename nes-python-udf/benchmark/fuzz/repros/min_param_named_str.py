# codon-fuzz: entry=udf args=VARSIZED,VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["red,green,blue", ","]]
# Codon fork 3b8c3a9 gave 1: a parameter named str shadowed nothing, so str.split(delim) called str.split on delim.
# Found in the real UDF databases/redshift_periscope_split_count.


def udf(str, delim):
    return len(str.split(delim))
