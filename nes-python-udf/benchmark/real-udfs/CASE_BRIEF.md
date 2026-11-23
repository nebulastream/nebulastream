# Brief: turn collected real UDFs into test cases

Repository root: /data-ssd/lukas/claude-worktrees/nebulastream-a028c2b88b466dcf/python-udfs-perf-eval-c8fd23 (call it REPO).
Collected UDFs: SCRATCH = /tmp/claude-2545/-data-ssd-lukas-claude-worktrees-nebulastream-a028c2b88b466dcf-python-udfs-perf-eval-c8fd23/af5b8720-7207-407f-82df-37469afa0a76/scratchpad/real-udfs
Each origin has SCRATCH/<origin>/catalog.jsonl and SCRATCH/<origin>/code/<id>.py (header with URL, commit, license).

Goal: for every entry of YOUR origin that has a code file, write one case module
REPO/nes-systests/udf/pyudfs/real_udfs/<origin>/<id>.py, or record why not. The cases will run (1) in NES as
out-of-process Python UDFs (real CPython 3.14 in a separate process) and (2) compiled by Codon, and both are compared with
CPython. Template, already done and checked: REPO/nes-systests/udf/pyudfs/real_udfs/literature/tuplex_zillow_extract_bd.py

A case module is:
1. Header lines first, exactly in this form:
   `# codon-fuzz: entry=<function> args=<T1>,<T2>,... returns=<T>` with NES types VARSIZED (str), INT64 (int),
   FLOAT64 (float), BOOLEAN (bool). Nothing else: no lists, dicts, bytes or None-only arguments.
   `# codon-fuzz-rows: 0`
   `# codon-fuzz-inputs: <JSON list of rows>`: 5 to 10 REALISTIC rows for what the UDF is meant to process (real-looking
   URLs, log lines, addresses, JSON documents, timestamps, prices, coordinates, ...). Cover the UDF's branches where you
   can. Every row must run without an exception in CPython (an exception fails the whole NES query) and give a value of
   the declared return type. No NULLs. One JSON line.
   `# real-udf: origin=<origin> id=<id> requires=<comma-separated third-party packages, or empty> wrapped=<0|1> ported=<0|1>`
   Optional extra `# Wrapper: ...` / `# Ported: ...` comment lines saying what you changed and why.
2. Then the code file's content unchanged, including its provenance header (URL, commit, license).
3. If needed, a wrapper at the end, `def case_entry(...)`, and `entry=case_entry` (set wrapped=1). Use a wrapper only to
   adapt the interface: build a dict/row/object the UDF expects from scalar arguments, pass constants the UDF takes as
   configuration, convert a non-scalar result to text (`json.dumps(result, sort_keys=True)` or `"|".join(...)`), turn a
   generator/flat_map into one string, or call a method of a class (DoFn.process, MapFunction.map, ScalarFunction.eval).
   Never change the UDF's logic.
4. Python 2 code: port only the syntax that Python 3 rejects or that behaves differently (`except X, e`, print statement,
   `xrange`, `iteritems`, `urlparse` module, integer `/`), as little as possible, and set ported=1 with a `# Ported:` line.

Check each case with: `python3 -I REPO/nes-python-udf/benchmark/real-udfs/check_case.py <case files>` (system Python 3.14,
standard library only). It must print OK. For a case whose third-party packages are not installed (requires= not empty),
the check fails on import: write the case anyway with your best realistic inputs, and list it as unverified.

Do NOT write a case (record it in REPO/nes-python-udf/benchmark/real-udfs/skipped/<origin>.jsonl as
{"id", "reason"}) when the UDF:
- only calls an ML model, or needs network, a database, Redis, files that are not part of the code, or a Spark/Beam
  runtime to execute;
- is not per-row (a whole table, batch/Arrow/pandas Series logic you cannot express per value, a stateful aggregation);
- takes or returns something that cannot reasonably map to the scalar types above even with a wrapper;
- is a duplicate of another case (same logic).
Also skip ids starting with `kaggle_` (license unclear) with reason "license".

Rules: write only inside REPO/nes-systests/udf/pyudfs/real_udfs/<origin>/. Do not touch other files, do not git
add/commit, do not install packages, do not run NES. Final report: number of cases written, how many check OK, how many are
unverified because of third-party packages (list the packages), number skipped by reason, and any UDF whose code you think
is buggy in CPython itself.
