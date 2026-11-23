# codon-fuzz: entry=case_entry args=INT64,INT64,INT64,INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0, 0, 100, 100], [50, 50, 100, 100], [37, 61, 100, 100], [10, 90, 100, 100], [64, 20, 100, 100], [49, 49, 100, 100], [5, 5, 10, 100]]
# real-udf: origin=streaming id=beam_julia_point_color requires= wrapped=1 ported=0
# Wrapper: case_entry builds the (x, y) element, passes the example's constant c = complex(-.62772, .42193) and returns the (x, y, iterations) tuple as "x|y|i".

# Source: https://github.com/apache/beam/blob/7f2fd8b6b080ad65ee49a2c75045fc34bec7685c/sdks/python/apache_beam/examples/complete/juliaset/juliaset/juliaset.py#L31-L44
# Repository: https://github.com/apache/beam (commit 7f2fd8b6b080ad65ee49a2c75045fc34bec7685c)
# License: Apache-2.0
# System: Apache Beam
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def from_pixel(x, y, n):
  """Converts a NxN pixel position to a (-1..1, -1..1) complex number."""
  return complex(2.0 * x / n - 1.0, 2.0 * y / n - 1.0)


def get_julia_set_point_color(element, c, n, max_iterations):
  """Given an pixel, convert it into a point in our julia set."""
  x, y = element
  z = from_pixel(x, y, n)
  for i in range(max_iterations):
    if z.real * z.real + z.imag * z.imag > 2.0:
      break
    z = z * z + c
  return x, y, i  # pylint: disable=undefined-loop-variable


def case_entry(x, y, n, max_iterations):
    return "|".join(str(value) for value in get_julia_set_point_color((x, y), complex(-.62772, .42193), n, max_iterations))
