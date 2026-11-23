"""Arithmetic kernels for the comparison of Codon's C numerics and Python numerics (PYTHON_COMPAT).

Each kernel runs 64 loop iterations per call, so the arithmetic and not the call dominates. The values stay small, so the
results do not overflow 64 bits. x takes negative values, where // and % of the two modes differ.
"""


def int_floordiv_mod(a, b):
    total = 0
    x = a
    for i in range(1, 65):
        total += x // i + x % (b + i)
        x = (x * 31 + i) % 1000003 - 500000
    return total


def int_add_mul(a, b):
    total = 0
    x = a
    for i in range(1, 65):
        total += x * i + b
        x = (x * 31 + i) & 0xFFFFF
        x -= 500000
    return total


def float_div(f, b):
    total = 0.0
    x = f
    for i in range(1, 65):
        total += x / (i * 0.5) + x / (b + i)
        x = x * 0.999 + 1.0
    return total


def float_floordiv_mod(f, b):
    total = 0.0
    x = f
    for i in range(1, 65):
        total += x // (i * 0.75) + x % (b + 0.5)
        x = x * 0.999 - 3.0
    return total
