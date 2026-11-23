# codon-fuzz: entry=fix_zip_codes args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["10027"], ["11201-3810"], ["10001"], ["11372-2034"], ["10463"], ["112345"]]
# real-udf: origin=literature id=tuplex_311_fix_zip_codes requires= wrapped=0 ported=0
# Inputs: empty or 00000 zips return None (NULL); the rows avoid them.

# Source: Tuplex: Data Science in Python at Native Code Speed (Spiegelberg et al., SIGMOD 2021)
# URL: https://github.com/tuplex/tuplex/blob/5380aec8ab38017401610435511450cda9fd8b3b/benchmarks/311/runtuplex.py#L29-L39
# Repository: https://github.com/tuplex/tuplex at commit 5380aec8ab38017401610435511450cda9fd8b3b
# License: Apache-2.0
# Path: benchmarks/311/runtuplex.py, lines 29-39

def fix_zip_codes(zips):
    if not zips:
        return None
    # Truncate everything to length 5
    s = zips[:5]

    # Set 00000 zip codes to nan
    if s == "00000":
        return None
    else:
        return s
