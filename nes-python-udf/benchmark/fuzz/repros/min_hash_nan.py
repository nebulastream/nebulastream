# codon-fuzz: entry=udf args=FLOAT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[NaN]]
# With C numerics, hashing NaN never returns (float.__hash__ loops on the NaN mantissa).
# With NES_CODON_PY_NUMERICS=1, every float hash raises ZeroDivisionError (1.0 / 0.0 in float.__hash__).
def udf(x):
    return len({x})
