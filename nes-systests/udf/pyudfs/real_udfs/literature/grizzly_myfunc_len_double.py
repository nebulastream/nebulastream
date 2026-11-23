# codon-fuzz: entry=myfunc args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Ilmenau"], ["Erfurt"], ["Berlin"], [""], ["Thuringia"], ["Gera"]]
# real-udf: origin=literature id=grizzly_myfunc_len_double requires= wrapped=0 ported=0

# Source: Putting Pandas in a Box -- Grizzly Python-to-SQL transpiler (Hagedorn, Kläbe, Sattler, CIDR 2021)
# URL: https://github.com/dbis-ilm/grizzly/blob/9149b11d85540d1858a72c2449e3a836e0740bcb/df_test.py#L1217-L1220
# Repository: https://github.com/dbis-ilm/grizzly at commit 9149b11d85540d1858a72c2449e3a836e0740bcb
# License: MIT
# Path: df_test.py, lines 1217-1220

def myfunc(i: str) -> int:
  l = len(i)
  l = l + l
  return l
