# codon-fuzz: entry=prioToInt args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1-URGENT"], ["2-HIGH"], ["3-MEDIUM"], ["4-NOT SPECIFIED"], ["5-LOW"]]
# real-udf: origin=literature id=grizzly_prio_to_int requires= wrapped=0 ported=0

# Source: Putting Pandas in a Box -- Grizzly Python-to-SQL transpiler (Hagedorn, Kläbe, Sattler, CIDR 2021)
# URL: https://github.com/dbis-ilm/grizzly/blob/9149b11d85540d1858a72c2449e3a836e0740bcb/grizzly/it/resources/scripts/grizzly_udf.py#L1-L6
# Repository: https://github.com/dbis-ilm/grizzly at commit 9149b11d85540d1858a72c2449e3a836e0740bcb
# License: MIT
# Path: grizzly/it/resources/scripts/grizzly_udf.py, lines 1-6

def prioToInt(prio: str) -> int:
  if prio is None or len(prio) == 0:
    return None

  num = prio[0]
  return int(num)
