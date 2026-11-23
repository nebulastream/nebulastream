# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["en-US", 3], ["x", -2], ["de", 0]]
# Before Codon fork cd4d924+, d.get(k, None) raised 'optional unpack failed' for a present key (real UDF
# tlhop_process_lang), and str * n with n < 0 allocated a negative size and ended the worker (redshift str_multiply).
D = {"en": "English"}


def udf(s, n):
    name = D.get(s[:2], None)
    return ("none" if name is None else name) + "|" + s * n
