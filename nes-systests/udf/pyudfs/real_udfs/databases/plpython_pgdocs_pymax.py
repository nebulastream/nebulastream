# codon-fuzz: entry=pymax args=INT64,INT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[3, 7], [10, 2], [-5, -9], [42, 42], [0, -1], [1700000000, 1699999999]]
# real-udf: origin=databases id=plpython_pgdocs_pymax requires= wrapped=0 ported=0
# Source: https://www.postgresql.org/docs/current/plpython-funcs.html
# Commit: n/a (documentation page, PostgreSQL 18)
# License: PostgreSQL License (permissive, BSD-like)
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.

def pymax(a, b):
    if (a is None) or (b is None):
        return None
    if a > b:
        return a
    return b
