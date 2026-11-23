# codon-fuzz: entry=mymod args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Ilmenau"], ["Erfurt"], ["Berlin"], [""], ["Thuringia"], ["Gera"]]
# real-udf: origin=literature id=grizzly_mymod requires= wrapped=0 ported=0

# Source: Putting Pandas in a Box -- Grizzly Python-to-SQL transpiler (Hagedorn, Kläbe, Sattler, CIDR 2021)
# URL: https://github.com/dbis-ilm/grizzly/blob/9149b11d85540d1858a72c2449e3a836e0740bcb/df_test.py#L407-L408
# Repository: https://github.com/dbis-ilm/grizzly at commit 9149b11d85540d1858a72c2449e3a836e0740bcb
# License: MIT
# Path: df_test.py, lines 407-408

def mymod(s: str) -> int:
  return len(s) % 2
