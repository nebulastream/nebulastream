# codon-fuzz: entry=case_entry args=FLOAT64,FLOAT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0.0, 0.0], [-1.0, 0.0], [-0.75, 0.1], [1.0, 1.0], [0.3, 0.6], [-2.0, 0.0], [-0.1, 0.65], [0.26, 0.0]]
# real-udf: origin=streaming id=wdm_check_mandelbrot requires=apache-flink wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.
# Wrapper: case_entry returns the Row fields joined with "|", or "" when the UDF returns None.
# Imports: the original module's imports that this code uses.

import sys

from pyflink.table import DataTypes, Row

# Source: https://github.com/wdm0006/flink-python-examples/blob/b0a55a8efe82476244bc3b410729ba33be65cf06/mandelbrot/mandelbrot_set.py#L11-L96
# Repository: https://github.com/wdm0006/flink-python-examples (commit b0a55a8efe82476244bc3b410729ba33be65cf06)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def check_c(real_part, imag_part, max_iter=500):
    """Checks whether or not c is in the Mandelbrot Set.

    Accepts real and imaginary parts as separate floats.
    Returns a Row (real, imag, magnitude) if the orbit of z = z**2 + c stays
    within radius 2 for max_iter iterations, otherwise None (or None on failure).
    """
    try:
        c_complex = complex(float(real_part), float(imag_part))
        z = 0j

        for _ in range(max_iter):
            z = z * z + c_complex
            if abs(z) > 2:
                return None

        return Row(real_part, imag_part, float(abs(z)))

    except Exception as e:
        # Log error or handle it appropriately
        print(f"Error in check_c for ({real_part}, {imag_part}): {e}", file=sys.stderr)
        return None


def check_mandelbrot_udf(real_part, imag_part):
    return check_c(real_part, imag_part)


def case_entry(real_part, imag_part):
    row = check_mandelbrot_udf(real_part, imag_part)
    return "" if row is None else "|".join(str(value) for value in row)
