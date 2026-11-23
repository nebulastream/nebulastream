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


def float_pow_int(f, b):
    total = 0.0
    x = f * 1e-6
    for i in range(1, 65):
        total += x ** 2 + (x * 0.5) ** 3
        total += int(x * 1000.0)
        x = x * 0.999 + 0.001
    return total


def float_list_contains(f, b):
    values = [f, f * 0.5, f * 0.25, f * 0.125, f + 1.0, f - 1.0, f * 2.0, f * 3.0]
    total = 0
    x = f
    for i in range(1, 65):
        if x in values:
            total += 1
        total += values.count(x * 0.5)
        x = x * 0.5 if i % 2 == 0 else f
    return float(total)
