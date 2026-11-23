# Codon vs CPython: differential test findings

Harness: `codon_diff.py` (cases with a signature header, run in CPython and with `nes-python-udf-runner`). Codon cases
compile with PYTHON_COMPAT (`--python-compat 1`, the default); `--python-compat 0` gives Codon's C numerics.
Campaign 1 used `NES_CODON_PY_NUMERICS=1`, which PYTHON_COMPAT replaced.
Repros: `repros/`, one minimized case per finding. Run them all with:

```bash
python3 nes-python-udf/benchmark/fuzz/codon_diff.py nes-python-udf/benchmark/fuzz/repros --results /tmp/repros.jsonl
```

## Campaign 1 (2026-11-22, commits 271896acf3 to 5ed275defa)

Inputs: 222 builtin expressions (`gen_builtins.py`, E84) and 80 UDF programs that two subagents wrote (text; numbers,
control flow and containers). All runs use `NES_CODON_PY_NUMERICS=1` unless a row says otherwise.

### Crashes and hangs (a worker stops or never returns)

Fixed on 2026-11-23 in the Codon fork (branch nes-python-compat) and in NES; the last column names the fork commit.

| Repro | Expression | Codon | Numerics | Fixed |
|---|---|---|---|---|
| min_int_floordiv_zero.py | `a // 0` (int); smallest int `// -1` | SIGFPE | C only; both | 7164a15 |
| min_bin_negative.py | `bin(-5)`, `hex`, `oct` | never returns: `_int_format` loops on `b //= base` | Python only | 641f329 |
| min_hash_nan.py | `len({float('nan')})`, NaN as a dict key | never returns: `float.__hash__` loops on the NaN mantissa | C | d2796f3 |
| min_float_hash.py | `hash(x)` of any float, float set or dict keys | ZeroDivisionError from `1.0 / 0.0` in `float.__hash__` | Python only | d2796f3 |
| min_gen_list_twice.py | `list(g); list(g)` on one generator | SIGSEGV | both | ae5373a |
| min_dict_unpack_crash.py | `{**d, "x": 0}` | compiler abort (SIGABRT) | both | 9dd43c0 |

### Wrong values

| Area | Repro or expression | CPython | Codon |
|---|---|---|---|
| int division | `a // b`, `a % b` with a negative operand (C numerics) | floor | truncation |
| float to str (fixed: NES `seq_str_float`) | `str(x)`, `repr(x)`, f-strings without a spec, json.dumps | `0.0`, `0.3333333333333333` | `0`, `0.333333` (%g) |
| format specs | `f'{n:,}'`, `f'{x:,.2f}'`, `f'{n:08d}'`, `f'{x:10.4}'` | grouping, zero fill, `g` default | no grouping, space fill, other default |
| format specs | `f'{x:5.1f}%'` with inf | `  inf%` | `inf  %` |
| round | `round(2.675, 2)`, `round(0.5)` | 2.67, 0 (correctly rounded, half even) | 2.68, 1 |
| round | `round(n)` on an int | int | float |
| powers | `10 ** -1`, `0 ** -1`, `pow(3, -1, 7)` | 0.1, ZeroDivisionError, 5 | 0, inf, error |
| int and float | `float(n) == n` for 2**53+1; `n / 7` for large n | False; correctly rounded | True; 1 ulp off |
| NaN | `x in [x]` (fixed with PYTHON_COMPAT, 3b8c3a9), `(x,) == (x,)`, `min(x, nan)`, `max(nan, 0.0)` | identity first | IEEE compare only |
| closures | `[lambda: k for k in range(3)][0]()` | 2 (late binding) | 0 |
| hash | `hash(-1) == hash(-2)` | True | False |
| int() and float() parse | `int('+7')`, `int('1_000')`, `int('0', 0)`; `float('0x1f')` | values; ValueError | ValueError; 31 |
| str literals | `"\U0001f600"`, `"\N{...}"` | the character | the escape text |
| re | zero-width matches: `re.sub(r'$', ...)`, `re.sub(r'(?=\d)', ...)`, `re.findall(r'\b', ...)` | one match per position | extra matches |
| re | `re.findall` with one group | the group | the whole match |
| re | `re.findall(r'(a)(b)?', 'a')` | `('a', '')` | IndexError |
| re | `re.VERBOSE`; `(?i)` with `İ`, `ı`; `re.escape` | as Python | ignored; no match; escapes every non-alphanumeric |
| json | duplicate keys; `[2.0]`; `1e400`, `NaN`; lone surrogates | last key wins; `[2.0]`; inf, nan; accepted | all keys kept; `[2]`; error; error |
| float errors (fixed with PYTHON_COMPAT, 3b8c3a9) | `x ** 2` overflow, `(-x) ** 0.5`, `math.fmod(inf, 3)`, `int(inf)` | OverflowError, complex, ValueError | inf, nan, nan, INT64_MIN |

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

## Codon and Numba findings by cause and fix location (2026-11-23)

Owner: where a fix goes. "Fork" is ls-1801/codon (Codon stdlib and compiler). "Codon plugin" is `codon-plugins/` (re on
RE2, json on simdjson, unicode, compat). "nes_numba_ext" is `numba/python/nes_numba_ext/`. "Numba" is upstream Numba
(we can override it from nes_numba_ext). "Design" means fixed-width values, which a fix cannot remove without a cost.

| # | Cause | Codon | Numba | Owner | Fix |
|---|---|---|---|---|---|
| 1 | Process-ending bugs | int `//` 0 SIGFPE (C numerics); bin/hex/oct hang and float hash error (Python numerics); NaN hash hang; generator consumed twice SIGSEGV; `{**d}` SIGABRT | `str(-2**63)` throws std::bad_alloc | Fork; nes_numba_ext or Numba | Fix each; highest priority |
| 2 | Float to str | `%g`: `0`, `0.333333` | `<object type:float64>` | Fork; nes_numba_ext | One shortest round-trip formatter in the NES runtime, called by both |
| 3 | Math and float errors do not raise | `x ** 2` overflow, `(-x) ** 0.5`, `fmod(inf)`, `int(inf)` | `log(0)`, `sqrt(-1)`, `exp(1000)` | Fork; nes_numba_ext | Check the domain and raise ValueError or OverflowError |
| 4 | Int and float mix | `10 ** -1` is 0; `float(n) == n` for 2**53+1; `n / 7` 1 ulp off; `round(n)` gives float | `10 ** -1` is 0; `float(n) == n` | Fork; Numba | Negative exponent gives float; exact int/float compare; correctly rounded int division |
| 5 | Integer `//` and `%` | truncation with C numerics | floor (correct) | Codon default | Make Python numerics the default: E83 cost 1.01x on UDFBench, 3.3x on a float `//` kernel |
| 6 | Rounding and format specs | `round(2.675, 2)`, half-even, `,` grouping, zero fill, `g` default | specs mostly not supported | Fork | Port CPython's float formatting (shares the formatter of row 2) |
| 7 | NaN identity in containers | `x in [x]`, `(x,) == (x,)` | same | Fork; Numba | Identity check before `==` |
| 8 | str and int parsing | `int('+7')`, `int('1_000')`, `int('0', 0)`, `float('0x1f')` | `int('٣٤')` raises (our `int(str)` overload) | Fork; nes_numba_ext | Follow CPython's grammar, including Unicode digits |
| 9 | str methods | clean | `split(None, maxsplit)`, `replace('', x)`, `chr(255)` | Numba | Override in nes_numba_ext |
| 10 | re | zero-width matches, `findall` with one group, `(a)(b)?`, VERBOSE, `re.escape` | not checked against re_ext yet (literal patterns only) | Codon plugin (RE2); nes_numba_ext | Python's empty-match rules in the plugin's loop |
| 11 | json | duplicate keys, `[2.0]` prints `[2]`, `1e400`/`NaN` rejected | not checked yet | Codon plugin; nes_numba_ext | Last key wins; float repr from row 2; accept the CPython extensions |
| 12 | Language details | late-binding closures, `hash(-1)`, `\U`/`\N{}` literals | n/a | Fork | Low priority, except the str literals |

Status 2026-11-23: rows 1 (Codon) and 2 (Codon) are fixed in both modes; rows 3 (Codon `**`, `int(float)`, `fmod`) and 7
(lists) are fixed with PYTHON_COMPAT; row 5 is PYTHON_COMPAT. The other rows are documented in
`docs/design/20260708_Scalar_UDF_Support.md` (Extension — Python compatibility of INLINE UDFs), as are the Numba
limitations.

Rows 2, 3, 6 and 7 come back in both compilers. A fix in a shared C helper (float formatting, the math domain checks)
serves both. Rows 4 and 5 are partly design: an exact fix costs time on every operation, so measure each as E83 did.

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

## Codon with PYTHON_COMPAT after the fixes (E88, 2026-11-23, Codon fork 3b8c3a9)

Same cases as E85 (repros now 34). `results/E88-diff-codon-compat.jsonl`.

| Group | ok | diff | reject | crash | timeout | E85 (ok, diff, reject, crash, timeout) |
|---|---|---|---|---|---|---|
| builtins (222) | 170 | 25 | 27 | 0 | 0 | 158, 34, 27, 0, 3 |
| repros (34) | 9 | 25 | 0 | 0 | 0 | 0, 28, 0, 2, 1 (31 repros) |
| programs (80) | 33 | 40 | 7 | 0 | 0 | 25, 47, 7, 1, 0 |

## Real UDFs in Codon (E89, 2026-11-23, Codon fork 3b8c3a9, PYTHON_COMPAT)

The 258 real-UDF cases (`nes-systests/udf/pyudfs/real_udfs`, realistic rows only): 220 have a CPython reference (the
other 38 need packages the system Python lacks). All 210 standard-library cases pass as EXECUTION OUT_OF_PROCESS
(`RealUdfsOutOfProcess.test`). Codon: 100 equal to CPython, 9 differ, 110 do not compile, 1 crash.
`results/E89-real-udfs-codon.jsonl`.

| Group | ok | diff | reject | crash |
|---|---|---|---|---|
| literature | 36 | 0 | 27 | 0 |
| github_spark | 28 | 7 | 31 | 1 |
| databases | 16 | 1 | 35 | 0 |
| streaming | 20 | 1 | 17 | 0 |

Not compiled (110), by first error:

| Cases | Cause | Examples |
|---|---|---|
| 37 | module not in Codon: urllib.parse 9, unicodedata 5, base64 5, hashlib 3, logging 3, struct 2, difflib, argparse, xml.etree, and the packages pandas, scipy, thefuzz, ua_parser, dateutil | |
| 16 | a name with several types (dynamic typing: a variable or return of int and str, Optional) | Tuplex Zillow, flights, logs, TPC-H Q19 |
| 16 | cannot typecheck a lambda or `self` without annotations | Babelfish lambdas `lambda rec, ctx: ...` |
| 13 | missing attribute: `str.format` 2, `datetime.strptime`, indexing a `map()` result, `json.Value.pop`, slicing a `json.Value` | |
| 7 | NES rejects an import inside a function | kubeflow, Redshift json_array_*, mysql_year* |
| 6 | missing name: `string.capwords`, `time.strftime`/`strptime`, `typing.AnyStr`, `datetime.timezone` | |
| 3 | syntax error in the token parser | tuplex_logs_parse_with_regex, trivadis_ball_possession, redshift_aws_f_format_number |
| 2 | unbound method call `str.title(x)`, `str.upper(x)` | |
| 2 | name not defined: lazy annotation `Column`, `eval` | |
| 1 each | `%` formatting, `math.atan2` (no NES ABI entry), `print` (seq_print_full), numpy on json.Value, `int + str` (a real bug) | |

Different results (9) and crash (1):
- f-string specs: `{x:,.2f}` (no grouping), `{n:06d}` (no zero fill).
- `re.findall` with one group gives the whole match (2 cases).
- `json.loads('NaN')`, `float('1_000.5')`, `datetime.fromisoformat` with an offset.
- `optional unpack failed` (tlhop_process_lang).
- A parameter named `str` shadows the type (repros/min_param_named_str.py).
- SIGSEGV on `(x and float(x)) / 2.0` with an Optional x (repros/min_and_optional_crash.py).

## Codon bug fixes for real UDFs (E90, 2026-11-23, Codon fork cd4d924)

Fixes (fork commits 1f54ebc to cd4d924, NES 65cadfbf96 and 4385d73730):
- Crash (SIGSEGV) when the persistent compiler removed the IR types of a UDF with two types of one name (any Union).
- `x or y` / `x and y` with an Optional operand built a Union with an Optional member: wrong values and "invalid
  union call" (`name or "-"`, `d.get(k) or "-"`, `(x and float(x)) / 100`).
- A variable named `str`, `int` or `float` was the class in attribute access (`str.split(sep)` split `sep`), and
  f-strings called a user variable named `str` or `repr`.
- The token parser rejected a keyword directly followed by `(` (`if(x):`, `return(x)`, `raise(e)`, `while(...)`).
- A bare lambda statement did not type check; unpacking a generator (`a, b = map(...)`) and `x in generator` /
  `x in d.keys()` did not compile.
- NES: `seq_str_str` (format spec on a str) and the libm functions of `math` (tan, atan2, asin, sinh, erf, hypot, ...)
  were not allowed.

Results (`results/E90-*.jsonl`): real UDFs 117 ok, 8 diff, 95 reject, 0 crash (E89: 100, 9, 110, 1); differential
cases 230 ok, 96 diff, 17 reject of 343 (E88: 212, 90, 34 of 336). No case that was ok before fails now.

Known, not fixed: a class with inferred fields used as an annotation of another class's `__init__` parameter
(`class D: def __init__(self, v: str): self.v = v` and `class Rec: def __init__(self, d: D): self.d = d`) cannot be type
checked: Codon makes D generic over its inferred fields. Declaring the fields (`v: str`) avoids it. A Union with an
Optional member is still broken when a UDF writes it explicitly.

## Second batch of bug fixes (E91, 2026-11-23, Codon fork 9256f72)

Fixes (fork 0fceea3 to 9256f72; NES c20b3c9032, 38f0020336, 8e7a7705c1, a310b201fa):
- `str * n` with `n < 0` allocated a negative size and ended the worker (fork and NES unicode plugin).
- `d.get(k, None)` raised "optional unpack failed", also for a present key.
- A function that returns an int and a float (`return 1` and `return 1 - d / m`) did not compile; it now returns float.
- re plugin: an empty match was taken twice (`re.sub(r"$", "<", "a")` gave `a<<`); `findall` with one group gave whole
  matches; `"|".join(re.split(...))` did not compile.
- `int()` and `float()` of a str: `+7`, `1_000`, Unicode digits and spaces, `float("1e400")`; `float("0x1f")` was accepted.
- Format specs: the zero flag (`{n:06d}`), `,` grouping, and a spec starting with a sign (`{n:+d}`, a syntax error).

Results (`results/E91-*.jsonl`): real UDFs 125 ok, 2 diff, 93 reject (E90: 117, 8, 95); differential cases 252 ok,
80 diff, 15 reject of 347 (E90: 230, 96, 17 of 343).

Changes in three differential cases that were ok by chance: `findall` with more than one group now raises
NotImplementedError (it gave whole matches; Python gives tuples, a type that depends on the pattern), so c07 and c31
differ; `re.split` gives "" for a group that did not take part in the match (Python: None), so c15 differs.

Known, not fixed: rebinding a str variable to None (`t = s; t = None`) fails at run time (the variable's type is fixed
at its first assignment); `d.get(k, opt)` with an Optional variable as default; a return annotation that the body does
not follow (`-> int` returning a float); format specs `_` grouping, `%` type and a precision without a type
(`{x:10.4}`); `$` before a trailing newline, `re.VERBOSE`, `(?i)` with `İ`; json `NaN`/`Infinity`/`1e400`.

## CPython's pure-Python library in Codon UDFs (2026-11-23, experiment)

NES_CODON_PYTHON_PATH (':'-separated) adds pure-Python libraries, e.g. CPython's stdlib, as the last import paths of
Codon UDFs: Codon's stdlib and the NES plugins come first, a library module fills in what they lack. In such modules,
imports are not implicitly relative (Python 3), and NES's compat import (bytes, bytearray, frozenset) is prepended as in
UDF modules. Off unless the variable is set.

Blockers met while compiling `urllib.parse` for the real UDF redshift_periscope_url_domain (a scratch copy of the module
was patched past each one to find the next). Every top-level statement of the module must compile, also those the UDF
does not use.

| # | Blocker | Status / root-fix cost |
|---|---|---|
| 0 | `def f(a, *, b)`, `def f(a, /, b)` | fixed (fork 0bbf260) |
| 1 | a nested def of the same name in both branches of an if/else (scoping: cannot bind) | fixed (fork 8c4be42) when the condition is static, as `isinstance(qs, str)` here |
| 2 | `import types`: `try: from _types import *` (C module) with an introspection fallback | fixed: try-import fallback (fork 683b2fd), Codon `types` module (3c7b9a1) |
| 3 | `import warnings`: `_py_warnings` (registry, filters, class tricks) | shim (warn as a no-op or log): small |
| 4 | `import ipaddress`: `@functools.total_ordering` class decorator | fixed (fork 39010f8); ipaddress itself not tried yet |
| 5 | `namedtuple('X', 'a b')`: compiler crash on the string form | fixed (fork 465e7e2) |
| 6 | `X.__doc__ = ...` | fixed (fork abc8b30) |
| 7 | `class R(_NamedTupleBase, _Mixin)`: reference class from a tuple class | fixed (fork 8f558f0) |
| 8 | a class with inferred fields as a base class: unbound generics, compiler assertion | partly fixed: fields typed from the annotated `__init__` parameters (fork 5506259); else an error, not an assertion (5d33e3f) |
| 9 | `_fix_result_transcoding`: class attributes assigned from outside at import time | dynamic; needs a source change or class variables |
| 10 | `bytes` not defined in library modules | fixed in NES (compat import also for library modules) |
| 11 | `bytes(frozenset(b'...'))`: NES compat bytes iterates as str, not int | compat bytes as Python's: medium |
| ... | not reached yet: `_coerce_args` (str/bytes), `Quoter(defaultdict)` with `__missing__` (`functools.lru_cache` is in fork 39010f8) | unknown |

With fork 7145bd8, the unmodified CPython `urllib.parse` stops at blocker 1, and the scratch copy at blocker 11. With
fork 8c4be42, it stops at blocker 2: `types.py` reads `_f.__code__` to get CodeType. With fork 3c7b9a1, it stops in
`ipaddress` at `@classmethod` (line 427), which Codon does not define.

## General Codon gaps (E92, 2026-11-23, Codon fork 7145bd8)

Fixes (fork 5506259 to 7145bd8), each with a repro in `repros/`:
- A namedtuple subclass with mixins (`class R(namedtuple(...), Mixin)`) is a tuple class with the namedtuple's fields;
  unpacking, indexing, `len`, `==` and `hash` work (`min_namedtuple_subclass_*`).
- The repr of a named class is `Name(x=1, y='a')` as in Python, not `(x: 1, y: 'a')` (`min_namedtuple_repr`).
- `from dataclasses import dataclass`, `@dataclasses.dataclass`; a dataclass has repr and `==` by default; `frozen`,
  `unsafe_hash`, `slots`, `match_args` are accepted (`min_dataclass`).
- `@functools.total_ordering` (compile time); `functools.reduce`, `cache`, `lru_cache` (the function itself),
  `cmp_to_key` (`min_total_ordering`, `min_functools`).
- `try: import X` / `except ImportError:` picks the branch at compile time: the handler if a module is not found, else
  the body (`min_import_fallback*`). Adds ImportError and ModuleNotFoundError.
- A string annotation is a forward reference: `other: "V"`, `List["Node"]` (`min_forward_reference`).
- `a != b` without `__ne__` is `not (a == b)`; `==` of a reference class without `__eq__` is identity (`min_ne_from_eq`).
- Inferred class fields take the annotation of the `__init__` parameter (5506259); an unannotated class with inferred
  fields as a base class is an error, not a compiler assertion (5d33e3f).

Results (`results/E92-*.jsonl`, same cases as E91): real UDFs 130 ok, 2 diff, 88 reject (E91: 125, 2, 93; the five
are Babelfish UDFs with annotated `__init__` parameters); differential cases 252 ok, 80 diff, 15 reject, no change.

Known, not fixed: a nested def of the same name in both branches of an if (urllib.parse blocker 1). The import rule
applies only to a `try` body of imports; in a body with other statements, a missing module is still a compile-time
error.

## A function defined on several paths (E93, 2026-11-23, Codon fork 8c4be42)

Python code defines a nested function of the same name in both branches of an if (urllib.parse `parse_qsl`:
`_unquote` for str and for bytes). Scoping must treat the name as a variable that each path updates, and Codon did not
allow that for a def ("cannot bind ... to global or nonlocal name").

Fix: a name with two or more defs in one scope, one of them inside an if, loop, try or with, gets each def renamed to
`_nes_def_<name>_<n>` followed by `<name> = _nes_def_<name>_<n>`. A recursive call uses the new name. The type checker
keeps only the taken branch of a static condition, so the variable has one function type. The IR translation crashed on
such a variable also without the rewrite (`if isinstance(s, str): g = f else: g = h`): its declaration has no right
side, and an update stores a function, which has no IR value. Both are skipped now, because calls through the
variable resolve to the function at compile time. Repros: `min_conditional_def_static`, `min_conditional_def_recursive`,
`min_conditional_function_alias`.

Limit: two defs on paths that can both run (a condition known only at run time) are different types. This gives the
error "function 'f' is defined differently on two paths that can run" (`min_conditional_def_dynamic`).

Results (`results/E93-*.jsonl`, E92 cases plus the repros): real UDFs 130 ok, 2 diff, 88 reject; differential cases
252 ok, 80 diff, 15 reject. No change from E92.

## The types module and __class_getitem__ (E94, 2026-11-23, Codon fork 3c7b9a1)

- `stdlib/types.codon`: `FunctionType`, `LambdaType`, `GeneratorType` and `NoneType` are Codon types. `CodeType`,
  `SimpleNamespace`, `GenericAlias`, `MethodType`, `ModuleType` and the other names are empty placeholder classes, so a
  module that imports or mentions them compiles. `new_class`, `prepare_class`, `coroutine` and the other run-time class
  helpers are not provided. Codon's stdlib comes before NES_CODON_PYTHON_PATH, so this module replaces CPython's
  `types.py`, which cannot compile (it finds types through `_f.__code__`, `sys.implementation` and similar objects).
- A `__class_getitem__` class variable (`classmethod(types.GenericAlias)`, which allows `C[int]` in type hints) is
  removed in scoping. The class keeps its inferred fields (a class variable alone would stop the inference).
- Repros: `min_types_module`, `min_class_getitem`. `isinstance(None, types.NoneType)` is False in Codon: the literal
  `None` has the type `Optional[NoneType]`.

Results (`results/E94-*.jsonl`): real UDFs 130 ok, 2 diff, 88 reject; differential cases 252 ok, 80 diff, 15 reject;
all repros as in E93. No change.

Next urllib.parse blocker: `ipaddress` uses `@classmethod` (Codon has `@staticmethod` only).
