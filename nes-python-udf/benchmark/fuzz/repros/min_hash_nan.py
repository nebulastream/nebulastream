# codon-fuzz: entry=udf args=FLOAT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[NaN]]
# Before Codon fork d2796f3, with C numerics, hashing NaN never returned (float.__hash__ looped on the NaN mantissa).
# Before Codon fork d2796f3, with Python numerics (PYTHON_COMPAT), every float hash raised ZeroDivisionError (1.0 / 0.0
# in float.__hash__).
def udf(x):
    return len({x})
