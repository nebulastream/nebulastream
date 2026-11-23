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

## The same cases in Numba, LPython and Babelfish (E85, 2026-11-22, commit 35c1eec406)

`codon_diff.py --compiler` runs a case in Numba (nes-python-udf-runner), LPython 0.22.0 (`lpython_driver.c`, the
harness adds the types from the header) and Babelfish (`FuzzCaseRunner` in the Babelfish port, GraalPy 25 in the query
engine). Cases: 222 builtins, 31 repros, 80 UDF programs (`programs/`). Codon uses Python numerics.
Results: `results/E85-diff-*.jsonl`. Summary: `compare_targets.py results/E85-diff-{codon,numba,lpython,babelfish}.jsonl`.

| Group | Target | ok | diff | reject | crash | timeout |
|---|---|---|---|---|---|---|
| builtins (222) | Codon | 158 | 34 | 27 | 0 | 3 |
| builtins | Numba | 129 | 43 | 50 | 0 | 0 |
| builtins | LPython | 15 | 55 | 152 | 0 | 0 |
| builtins | Babelfish | 200 | 22 | 0 | 0 | 0 |
| repros (31) | Codon | 0 | 28 | 0 | 2 | 1 |
| repros | Numba | 6 | 11 | 14 | 0 | 0 |
| repros | LPython | 0 | 0 | 31 | 0 | 0 |
| repros | Babelfish | 30 | 1 | 0 | 0 | 0 |
| programs (80) | Codon | 25 | 47 | 7 | 1 | 0 |
| programs | Numba | 3 | 5 | 72 | 0 | 0 |
| programs | LPython | 0 | 0 | 80 | 0 | 0 |
| programs | Babelfish | 68 | 12 | 0 | 0 | 0 |

The Codon repros differ by construction. A repro of Codon that another target runs equal to CPython shows that the
difference is Codon's own.

### Numba
- Rejects 72 of 80 programs (untyped containers, str methods, json and re are not in nopython mode).
- `str(float)` and `repr(float)` give `<object type:float64>`; a list as str gives `<object type:list(int64)...>`.
- `s.split(None, maxsplit)` ignores maxsplit; `s.replace('', x)` and `s.replace(t, x)` with an empty `t` insert twice.
- math functions do not raise: `math.log(0)` is -inf, `math.sqrt(-1)` is nan, `math.exp(1000)` is inf.
- `int('٣٤')` (non-ASCII digits) raises; `chr(255)` gives an invalid character.
- `10 ** -1` is 0, NaN identity in containers is lost, `float(n) == n` is True for 2**53+1 (as in Codon).
- `str(-2**63)` throws std::bad_alloc out of the UDF and ends the process (repros/min_numba_str_int64_min.py).

### LPython
- Rejects all 80 programs and all repros: every local needs a type, no try/except, no re or json, i32 literals do not
  mix with i64.
- Strings are bytes: indexing, slicing and reversing split UTF-8 sequences; case mappings and `is*` predicates are
  ASCII-only; `s + t` and `bool(s)` stop at a NUL byte.
- `strip`, `split` and `join` end the process on some inputs (an empty string).
- Integer `//` of large values goes through double (`n // 7` is off by 6); float `%` and `//` are wrong for large values.
- `str(float)` prints `%f` (`0.000000`); math functions do not raise; `abs(-0.0)` is -0.0.

### Babelfish (GraalPy 25)
- Runs every case. Most differences are GraalPy's own; a string with a NUL byte ends at the NUL in Babelfish's Text
  (`known-babelfish-text-ends-at-nul`).
- Unicode: `'ǅungla'.title()` is `ǅUngla`; `swapcase` gives final sigma `ς`; `splitlines` does not split at
  `\x1c`-`\x1e`; `json.loads` of a lone surrogate gives an empty string.
- Floats: `repr(5e-324)` is `4.9e-324`; `x // 0.3` is off by one (-4115 for -1234.5); `divmod(inf, 1.5)` is (inf, nan);
  `round(-45.9, -2)` is 0.0, not -0.0; `math.log(0, 3)` is -inf without an error; `math.frexp(nan)[1]` is -1.
- math functions differ from CPython by 1 ulp (log10, sin, erf, asinh, atan2, hypot and more).
- The recursion limit is lower (RecursionError where CPython returns).

## The same cases in PyPy (E86, 2026-11-23, commit ebaa7360f6)

`--compiler PyPy` runs the reference child of the harness in PyPy 7.3.20 (Python 3.11, `/usr/bin/pypy3`), the interpreter
of the out-of-process UDF experiments E19 to E59. The reference is CPython 3.14. Results: `results/E86-diff-pypy.jsonl`.

| Group | ok | diff | reject | crash | timeout |
|---|---|---|---|---|---|
| builtins (222) | 202 | 20 | 0 | 0 | 0 |
| repros (32) | 27 | 0 | 5 | 0 | 0 |
| programs (80) | 69 | 11 | 0 | 0 | 0 |

- `str.split()` without a separator does not split at `\x1c`-`\x1f`, but `'\x1c'.isspace()` is True. This one cause gives
  most of the differences (11 builtins, 8 programs).
- `s.count('')` counts UTF-8 bytes: `'Ångström'.count('')` is 11, CPython gives 9.
- `'%.3f' % x` for x of 1e50 or more gives the `%g` form (`1e+308`), as Python 2 did; `f'{x:.3f}'` is correct.
- `round(-0.0, 2)` and `math.gamma` of a large negative non-integer give 0.0, not -0.0.
- `x ** 2` overflow gives inf, not OverflowError.
- `math.log2` and `math.erf` differ by 1 ulp.
- `json.loads` of invalid non-ASCII input (`'٣٤'`) makes an error message with a lone UTF-8 byte. Formatting that error
  raises SystemError (an RPython CheckError), and the process ends. The 5 JSON repros are rejects for this reason.
- Python version: `re.subn(r'\B', '.', '')` matches once in CPython 3.14 and not in 3.11 (a 3.14 change, not PyPy's).
