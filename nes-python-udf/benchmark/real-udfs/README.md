# Real Python UDFs

511 Python UDFs from papers and benchmarks (literature), PySpark projects on GitHub (github_spark), database and warehouse
systems (databases) and stream processors (streaming), collected on 2026-11-23. `catalog.jsonl` has one line per UDF:
source, URL pinned to a commit, license, signature, tags, and `case` (the case module) or `case_skipped` (why there is
none). UDFs without a permissive license (or with only a description) have no case.

A case module (`nes-systests/udf/pyudfs/real_udfs/<origin>/<id>.py`) is the UDF's code unchanged with its provenance
header, a `# codon-fuzz:` header with its NES signature and realistic sample rows, and, where the interface needs it, a
`case_entry` wrapper (`wrapped=1`) or a minimal Python 2 port (`ported=1`). `skipped/<origin>.jsonl` lists the UDFs with
code that have no case. `CASE_BRIEF.md` is the brief the cases were written from.

- `check_case.py CASE...` runs the sample rows in this Python; every row must run (an out-of-process query fails on an
  exception).
- `gen_systest.py --out nes-systests/udf/RealUdfsOutOfProcess.test` writes the systest (run it with the Python of the UDF
  processes), one query per case with CPython's results. Cases whose packages this Python lacks are left out.
- `nes-python-udf/benchmark/fuzz/codon_diff.py nes-systests/udf/pyudfs/real_udfs/<origin>` compares Codon with CPython.
