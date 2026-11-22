"""Writes one codon_diff.py case per expression over the str, int and float builtins.

Every case has the arguments s, t (VARSIZED), n (INT64) and x (FLOAT64) and returns the expression's value in its NES type.
A list or tuple result is joined with \\x1e into a VARSIZED result. A case that Codon does not compile shows the builtin
is not supported, which codon_diff.py reports as codon-reject.

Usage: gen_builtins.py OUTPUT_DIR
"""

import os
import re
import sys

ARGS = "VARSIZED,VARSIZED,INT64,FLOAT64"
J = '"\\x1e".join'

# (expression, kind): kind is str, int, float, bool or list (a sequence of str).
TEMPLATES = [
    # case mappings and predicates
    *[(f"s.{m}()", "str") for m in ["lower", "upper", "casefold", "title", "capitalize", "swapcase"]],
    *[(f"s.{m}()", "bool") for m in ["isdigit", "isdecimal", "isnumeric", "isalpha", "isalnum", "isspace", "isupper",
                                     "islower", "istitle", "isidentifier", "isprintable", "isascii"]],
    # stripping and splitting
    *[(f"s.{m}()", "str") for m in ["strip", "lstrip", "rstrip"]],
    *[(f"s.{m}(t)", "str") for m in ["strip", "lstrip", "rstrip", "removeprefix", "removesuffix"]],
    ("s.strip(' ,.')", "str"),
    ("s.split()", "list"), ("s.split(t)", "list"), ("s.split(',')", "list"), ("s.split(None, n % 4)", "list"),
    ("s.split(t, n % 5 - 1)", "list"), ("s.rsplit()", "list"), ("s.rsplit(t, n % 4)", "list"),
    ("s.rsplit(None, n % 3)", "list"), ("s.splitlines()", "list"), ("s.splitlines(True)", "list"),
    ("list(s.partition(t))", "list"), ("list(s.rpartition(t))", "list"),
    # searching
    *[(f"s.{m}(t)", "int") for m in ["find", "rfind", "index", "rindex", "count"]],
    ("s.find(t, n % 10)", "int"), ("s.find(t, -3)", "int"), ("s.count(t, 1, n % 20)", "int"), ("s.count('')", "int"),
    ("s.startswith(t)", "bool"), ("s.endswith(t)", "bool"), ("s.startswith(t, n % 5)", "bool"), ("t in s", "bool"),
    # building strings
    ("s.replace(t, '#')", "str"), ("s.replace(t, '#', n % 4 - 1)", "str"), ("s.replace('', '-')", "str"),
    ("s.zfill(n % 30)", "str"), ("s.center(n % 30, '*')", "str"), ("s.ljust(n % 30, '.')", "str"),
    ("s.rjust(n % 30)", "str"), ("s.expandtabs(n % 9)", "str"), ("s * (n % 4)", "str"), ("s + t", "str"),
    ("t.join(s.split())", "str"), ("''.join(reversed(s))", "str"), ("''.join(sorted(s))", "str"),
    ("''.join(sorted(set(s)))", "str"), ("''.join(list(dict.fromkeys(s)))", "str"), ("s.title().swapcase()", "str"),
    # indexing and slicing
    ("s[n % 7:]", "str"), ("s[:n % 7]", "str"), ("s[n % 9 - 4:]", "str"), ("s[::-1]", "str"), ("s[::n % 4 - 2]", "str"),
    ("s[1:-1:2]", "str"), ("s[n % 5]", "str"), ("s[-1]", "str"), ("len(s)", "int"), ("len(s.encode())", "int"),
    ("ord(s[0])", "int"), ("chr(n % 1114112)", "str"), ("chr(n % 55296)", "str"), ("max(s)", "str"), ("min(s)", "str"),
    ("sum(ord(c) for c in s)", "int"), ("s < t", "bool"), ("s == t.lower()", "bool"), ("bool(s)", "bool"),
    ("[c for c in s if c.isalpha()]", "list"), ("[w.capitalize() for w in s.split(' ')]", "list"),
    # parsing
    ("int(s)", "int"), ("int(s, 16)", "int"), ("int(s, 0)", "int"), ("int(s.strip() or '0')", "int"), ("float(s)", "float"),
    ("int(float(s))", "int"), ("str(n).isdigit()", "bool"),
    # formatting
    ("str(n)", "str"), ("str(x)", "str"), ("repr(x)", "str"), ("repr(s)", "str"), ("str(n / 7)", "str"),
    ("bin(n)", "str"), ("hex(n)", "str"), ("oct(n)", "str"), ("f'{n:,}'", "str"), ("f'{n:08d}'", "str"),
    ("f'{n:+d}'", "str"), ("f'{n:x}'", "str"), ("f'{x:.3f}'", "str"), ("f'{x:e}'", "str"), ("f'{x:g}'", "str"),
    ("f'{x:.2%}'", "str"), ("f'{x:10.4}'", "str"), ("f'{x:,.2f}'", "str"), ("f'{s:>10}'", "str"),
    ("f'{s:^9}'", "str"), ("f'{s!r}'", "str"), ("format(x, '.10g')", "str"), ("'%d|%s' % (n, s)", "str"),
    ("'%.3f' % x", "str"), ("'{}-{}'.format(s, n)", "str"), ("str(x == x)", "str"), ("str(round(x, 2))", "str"),
    # integers
    ("n // 7", "int"), ("n % 7", "int"), ("n // -7", "int"), ("n % -7", "int"), ("-n // 3", "int"), ("n // (n % 5)", "int"),
    ("n % (n % 5)", "int"), ("divmod(n, 13)[0]", "int"), ("divmod(n, -13)[1]", "int"), ("(n % 1000) ** 3", "int"),
    ("pow(n % 1000, 3, 97)", "int"), ("abs(n)", "int"), ("-n", "int"), ("n >> (n % 64)", "int"), ("(n % 1000) << 20", "int"),
    ("n & -n", "int"), ("~n", "int"), ("n.bit_length()", "int"), ("int(n / 3)", "int"), ("round(n, -2)", "int"),
    ("max(n, -n, 7)", "int"), ("n == n % 1000", "bool"), ("int(True) + n % 3", "int"),
    # floats
    ("x / 3", "float"), ("x / (n % 3)", "float"), ("x // 0.3", "float"), ("x % 2.5", "float"), ("x % -2.5", "float"),
    ("divmod(x, 1.5)[0]", "float"), ("x ** 2", "float"), ("x ** 0.5", "float"), ("abs(x)", "float"), ("-x", "float"),
    ("round(x)", "int"), ("round(x, 2)", "float"), ("round(x, -2)", "float"), ("int(x)", "int"), ("float(n)", "float"),
    ("x + n", "float"), ("n / 7", "float"), ("x * 1e300", "float"), ("x.is_integer()", "bool"), ("x == int(x)", "bool"),
    ("x < n", "bool"), ("max(x, 0.0)", "float"), ("min(x, float('nan'))", "float"), ("float('-0.0') + x", "float"),
    # math
    *[(f"math.{f}(x)", "float") for f in ["sqrt", "log", "log10", "log2", "log1p", "exp", "expm1", "sin", "cos", "tan",
                                          "atan", "sinh", "tanh", "fabs", "erf", "gamma", "lgamma", "asinh"]],
    ("math.floor(x)", "int"), ("math.ceil(x)", "int"), ("math.trunc(x)", "int"), ("math.fmod(x, 3.0)", "float"),
    ("math.copysign(1.0, x)", "float"), ("math.isnan(x)", "bool"), ("math.isinf(x)", "bool"), ("math.hypot(x, n)", "float"),
    ("math.atan2(x, n)", "float"), ("math.pow(x, 3)", "float"), ("math.log(x, 3)", "float"), ("math.gcd(n, 360)", "int"),
    ("math.factorial(n % 20)", "int"), ("math.isclose(x, round(x))", "bool"), ("math.modf(x)[0]", "float"),
    ("math.frexp(x)[1]", "int"), ("math.ldexp(x, n % 10)", "float"),
    # re (Codon plugin)
    ("re.findall(r'\\w+', s)", "list"), ("re.sub(r'\\s+', ' ', s)", "str"), ("re.split(r'[,;]\\s*', s)", "list"),
    ("re.findall(r'\\d+', s)", "list"), ("re.sub(r'[^a-z]', '', s.lower())", "str"),
    ("bool(re.match(r'^[A-Z][a-z]+$', s))", "bool"), ("re.sub(r'(\\w)(\\w*)', r'\\2\\1', s)", "str"),
    ("re.findall(r'(?i)[aeiou]', s)", "list"), ("re.escape(s)", "str"),
    # containers
    ("sorted(s.split(), key=len)", "list"), ("sorted(s.split(), reverse=True)", "list"),
    ("sorted(s.lower().split(), key=lambda w: (-len(w), w))", "list"),
    ("[k for k, v in sorted({c: s.count(c) for c in s}.items(), key=lambda kv: -kv[1])]", "list"),
    ("list({w: 1 for w in s.split()})", "list"), ("[str(i) for i in range(n % 5, 10, 3)]", "list"),
    ("list(map(str, [len(w) for w in s.split()]))", "list"), ("[w for w in s.split() if len(w) > 2][:3]", "list"),
]

RETURNS = {"str": "VARSIZED", "list": "VARSIZED", "int": "INT64", "float": "FLOAT64", "bool": "BOOLEAN"}


def slug(expression, index):
    return f"b{index:03d}_" + re.sub(r"\W+", "_", expression).strip("_")[:40]


def main():
    output = sys.argv[1]
    os.makedirs(output, exist_ok=True)
    for index, (expression, kind) in enumerate(TEMPLATES):
        body = f"{J}({expression})" if kind == "list" else expression
        source = (f"# codon-fuzz: entry=udf args={ARGS} returns={RETURNS[kind]}\n"
                  f"# expression: {expression}\n"
                  + "".join(f"import {m}\n" for m in ("math", "re") if f"{m}." in expression) + "\n\n"
                  f"def udf(s, t, n, x):\n    return {body}\n")
        with open(os.path.join(output, slug(expression, index) + ".py"), "w", encoding="utf-8") as file:
            file.write(source)
    print(f"{len(TEMPLATES)} cases in {output}")


if __name__ == "__main__":
    main()
