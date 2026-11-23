# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["+7"], ["1_000"], ["  42  "], ["\u0663\u0664"], ["-1_0"], ["1__0"], ["_1"], ["0x1f"], ["1e400"], ["-1e400"], ["inf"], ["NaN"], ["-Infinity"], ["1_000.5"], ["\u00a012\u3000"], ["1e-400"], ["12a"], [""], ["+"], ["0b101"]]
# Before NES 38f0020336+ and Codon fork dd19436+, int() and float() rejected '+7', '1_000', Unicode digits and spaces,
# accepted float('0x1f'), and gave an error for float('1e400') (CPython: inf).


def conv(f, s):
    try:
        return repr(f(s))
    except ValueError:
        return "VE"


def udf(s):
    return conv(int, s) + "|" + conv(float, s)
