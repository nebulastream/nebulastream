# codon-fuzz: entry=jaro_winkler_similarity args=VARSIZED,VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["MARTHA", "MARHTA"], ["DWAYNE", "DUANE"], ["Jonathan Smith", "Jon Smith"], ["Katherine", "Catherine"], ["Muller", "Mueller"], ["abc", "xyz"], ["Robert", "Robert"]]
# real-udf: origin=github-spark id=ceja_jaro_winkler requires=jellyfish wrapped=0 ported=0
# Source: https://github.com/MrPowers/ceja/blob/d310525075d2adabbb52b4d6bdaca0f90663417d/ceja/functions.py#L47-L49
# Repository: MrPowers/ceja @ commit d310525075d2adabbb52b4d6bdaca0f90663417d
# License: MIT
# Copied verbatim from ceja/functions.py (L47-L49); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=FloatType())

import jellyfish as J


def jaro_winkler_similarity(s1, s2):
     return None if s1 == None or s2 == None else J.jaro_winkler_similarity(s1, s2)
