# codon-fuzz: entry=myfunc args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Ilmenau"], ["Erfurt"], ["Berlin"], ["Weimar"], ["Jena"], ["Gera"]]
# real-udf: origin=literature id=grizzly_myfunc_concat requires= wrapped=0 ported=0
# Inputs: the annotation says a: int, but a + "_grizzly" works only for str; the case passes str.

# Source: Putting Pandas in a Box -- Grizzly Python-to-SQL transpiler (Hagedorn, Kläbe, Sattler, CIDR 2021)
# URL: https://github.com/dbis-ilm/grizzly/blob/9149b11d85540d1858a72c2449e3a836e0740bcb/df_test.py#L774-L775
# Repository: https://github.com/dbis-ilm/grizzly at commit 9149b11d85540d1858a72c2449e3a836e0740bcb
# License: MIT
# Path: df_test.py, lines 774-775

def myfunc(a: int) -> str:
  return a+"_grizzly"
