# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a,b,c"], ["Grüße"], [""]]
# A format spec on a str needs seq_str_str, which the NES runtime lacked: the UDF did not compile.


def udf(s):
    return f"[{s:>10}][{s:<6}][{s:^9}][{s:*^9}]"
