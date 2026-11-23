# codon-fuzz: entry=reorder_name args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Andersson, Magdalena"], ["Löfven, Stefan"], ["Kristersson,Ulf"], ["Jimmie Åkesson"], ["Sjöstedt, Jonas"], ["af Malmborg, Hans"]]
# real-udf: origin=github-spark id=riksdagen_reorder_name requires= wrapped=0 ported=0
# Source: https://github.com/swerik-project/pyriksdagen/blob/2826a09ba8dcd10491cfef2b69c2c2fa12619ccd/pyriksdagen/mp.py#L284-L290
# Repository: swerik-project/pyriksdagen @ commit 2826a09ba8dcd10491cfef2b69c2c2fa12619ccd
# License: MIT
# Copied verbatim from pyriksdagen/mp.py (L284-L290); only the module's own import lines that the code uses are prepended.
# Applied per value through: polars-map_elements

def reorder_name(name):
    s = name.split(",")
    if len(s) == 1:
        return name
    else:
        newname = s[1].strip() + " " + s[0].strip()
        return newname
