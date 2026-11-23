# codon-fuzz: entry=nysiis args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Macintosh"], ["Knight"], ["Schmidt"], ["Phillipson"], ["Bishop"], ["Wheeler"], ["O'Daniel"]]
# real-udf: origin=github-spark id=ceja_nysiis requires=jellyfish wrapped=0 ported=0
# Source: https://github.com/MrPowers/ceja/blob/d310525075d2adabbb52b4d6bdaca0f90663417d/ceja/functions.py#L8-L10
# Repository: MrPowers/ceja @ commit d310525075d2adabbb52b4d6bdaca0f90663417d
# License: MIT
# Copied verbatim from ceja/functions.py (L8-L10); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

import jellyfish as J


def nysiis(s):
     return None if s == None else J.nysiis(s)
