# codon-fuzz: entry=udf args=VARSIZED,VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["red,green,blue", ","]]
# Codon (fork 3b8c3a9) gives 1: a parameter named str shadows the type, so str.split(delim) calls str.split on delim.
# Found in the real UDF databases/redshift_periscope_split_count.


def udf(str, delim):
    return len(str.split(delim))
