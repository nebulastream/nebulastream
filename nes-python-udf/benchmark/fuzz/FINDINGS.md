# Codon vs CPython: differential test findings

Harness: `codon_diff.py` (cases with a signature header, run in CPython and with `nes-python-udf-runner`).
Repros: `repros/`, one minimized case per finding. Run them all with:

```bash
NES_CODON_PY_NUMERICS=1 python3 nes-python-udf/benchmark/fuzz/codon_diff.py nes-python-udf/benchmark/fuzz/repros --results /tmp/repros.jsonl
```

## Campaign 1 (2026-11-22, commits 271896acf3 to 5ed275defa)

Inputs: 222 builtin expressions (`gen_builtins.py`, E84) and 80 UDF programs that two subagents wrote (text; numbers,
control flow and containers). All runs use `NES_CODON_PY_NUMERICS=1` unless a row says otherwise.

### Crashes and hangs (a worker stops or never returns)

| Repro | Expression | Codon | Numerics |
|---|---|---|---|
| (smoke test) | `a // 0` (int) | SIGFPE | C only |
| min_bin_negative.py | `bin(-5)`, `hex`, `oct` | never returns: `_int_format` loops on `b //= base` | Python only |
| min_hash_nan.py | `len({float('nan')})`, NaN as a dict key | never returns: `float.__hash__` loops on the NaN mantissa | C |
| min_float_hash.py | `hash(x)` of any float, float set or dict keys | ZeroDivisionError from `1.0 / 0.0` in `float.__hash__` | Python only |
| min_gen_list_twice.py | `list(g); list(g)` on one generator | SIGSEGV | Python |
| min_dict_unpack_crash.py | `{**d, "x": 0}` | compiler abort (SIGABRT) | both |

### Wrong values

| Area | Repro or expression | CPython | Codon |
|---|---|---|---|
| int division | `a // b`, `a % b` with a negative operand (C numerics) | floor | truncation |
| float to str | `str(x)`, `repr(x)`, f-strings without a spec, json.dumps | `0.0`, `0.3333333333333333` | `0`, `0.333333` (%g) |
| format specs | `f'{n:,}'`, `f'{x:,.2f}'`, `f'{n:08d}'`, `f'{x:10.4}'` | grouping, zero fill, `g` default | no grouping, space fill, other default |
| format specs | `f'{x:5.1f}%'` with inf | `  inf%` | `inf  %` |
| round | `round(2.675, 2)`, `round(0.5)` | 2.67, 0 (correctly rounded, half even) | 2.68, 1 |
| round | `round(n)` on an int | int | float |
| powers | `10 ** -1`, `0 ** -1`, `pow(3, -1, 7)` | 0.1, ZeroDivisionError, 5 | 0, inf, error |
| int and float | `float(n) == n` for 2**53+1; `n / 7` for large n | False; correctly rounded | True; 1 ulp off |
| NaN | `x in [x]`, `(x,) == (x,)`, `min(x, nan)`, `max(nan, 0.0)` | identity first | IEEE compare only |
| closures | `[lambda: k for k in range(3)][0]()` | 2 (late binding) | 0 |
| hash | `hash(-1) == hash(-2)` | True | False |
| int() and float() parse | `int('+7')`, `int('1_000')`, `int('0', 0)`; `float('0x1f')` | values; ValueError | ValueError; 31 |
| str literals | `"\U0001f600"`, `"\N{...}"` | the character | the escape text |
| re | zero-width matches: `re.sub(r'$', ...)`, `re.sub(r'(?=\d)', ...)`, `re.findall(r'\b', ...)` | one match per position | extra matches |
| re | `re.findall` with one group | the group | the whole match |
| re | `re.findall(r'(a)(b)?', 'a')` | `('a', '')` | IndexError |
| re | `re.VERBOSE`; `(?i)` with `İ`, `ı`; `re.escape` | as Python | ignored; no match; escapes every non-alphanumeric |
| json | duplicate keys; `[2.0]`; `1e400`, `NaN`; lone surrogates | last key wins; `[2.0]`; inf, nan; accepted | all keys kept; `[2]`; error; error |
| float errors | `x ** 2` overflow, `(-x) ** 0.5`, `math.fmod(inf, 3)`, `int(inf)` | OverflowError, complex, ValueError | inf, nan, nan, INT64_MIN |

### Clean areas

str case mappings and predicates (including the non-ASCII edge cases), strip/split/partition/find/replace/justify,
slicing, ord/chr, sorted with key and reverse (stable), set operations, dict order after deletes, try/except/else/finally,
the exception hierarchy, recursion, module-level state between rows, list slice assignment.

### Not supported (codon-reject)

`str.format`, `%` formatting, `format()`, f-string specs on str (`{s:>10}`), `{n:+d}`, `str.removeprefix/removesuffix`,
`dict.fromkeys`, `float.is_integer`, `round(x)` on a nullable float, `math` functions without a NES ABI entry (tan, atan,
atan2, sinh, tanh, asinh, erf, gamma, lgamma, log1p, expm1, hypot, ldexp), `re.split` with groups, `except (A, B) as e`,
bool arithmetic, heterogeneous lists, mutable default arguments, `tuple(list)`, user classes in sets or sorted with only
`__lt__`, `class E(ValueError): pass`, `math.comb/perm/isqrt/dist`, json.dumps keywords and mixed-type values.
