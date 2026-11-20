#!/usr/bin/env python3
"""Runs a sqllogictest file through NebulaStream's Calcite frontend and compares every result with SQLite.

The file is replayed in SQLite. Whenever a statement changes the tables, the current tables are written as CSV files and
registered as a new version of File sources (tab0_v2, ...); each query runs against the version that was current at its
position, as `CALCITE <query> INTO File(...);` in one embedded REPL session with --optimizer.finite_inputs=true. Results are
compared with SQLite's as sorted multisets of rows, with floats rounded to 3 decimals (the sqllogictest convention).

Usage: slt_run.py <slt file> <nes-repl-embedded> <nes-calcite-planner.jar> <java>
                  [--subqueries-only] [--limit N] [--workdir DIR] [--compiler]
"""
import argparse
import re
import socket
import sqlite3
import subprocess
import time
from collections import Counter
from pathlib import Path

SUBQUERY = re.compile(r'(?is)\b(?:IN|EXISTS)\s*\(\s*SELECT\b|(?:=|==|<>|!=|<=|>=|<|>)\s*\(\s*SELECT\b')
MODIFYING = re.compile(r'(?i)^\s*(INSERT|DELETE|UPDATE|REPLACE|CREATE\s+TABLE|DROP\s+TABLE)\b')


def records(path: Path):
    """Yields ('statement', sql) and ('query', sql), honouring skipif/onlyif for sqlite."""
    for block in path.read_text(errors='replace').split('\n\n'):
        lines = [line for line in block.strip().split('\n') if line and not line.startswith('#')]
        keep = True
        while lines and lines[0].split()[0] in ('skipif', 'onlyif'):
            kind, db = lines[0].split()[:2]
            keep = keep and ((kind == 'onlyif') == (db == 'sqlite'))
            lines = lines[1:]
        if not keep or not lines:
            continue
        head, body = lines[0].split(), lines[1:]
        if head[0] == 'statement':
            yield 'statement', '\n'.join(body)
        elif head[0] == 'query':
            end = body.index('----') if '----' in body else len(body)
            yield 'query', ' '.join(body[:end])


def nes_type(declared: str) -> str:
    declared = (declared or '').upper()
    if 'INT' in declared:
        return 'INT64'
    if any(k in declared for k in ('REAL', 'FLOA', 'DOUB', 'DECIMAL', 'NUMERIC')):
        return 'FLOAT64'
    return 'VARSIZED'


def csv_value(value) -> str:
    return '' if value is None else str(value)


def normalize(value) -> str:
    """One cell, from SQLite (Python value) or from our CSV output (string)."""
    if value is None or value == 'NULL':
        return 'NULL'
    if isinstance(value, str):
        if value in ('true', 'false'):
            return '1' if value == 'true' else '0'
        try:
            value = float(value) if any(c in value for c in '.eE') else int(value)
        except ValueError:
            return value
    if isinstance(value, float):
        return 'NULL' if value != value else f'{value:.3f}'
    return str(value)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(('localhost', 0))
        return s.getsockname()[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('slt', type=Path)
    parser.add_argument('repl', type=Path)
    parser.add_argument('jar', type=Path)
    parser.add_argument('java')
    parser.add_argument('--subqueries-only', action='store_true')
    parser.add_argument('--limit', type=int, default=1_000_000)
    parser.add_argument('--workdir', type=Path, default=Path('slt-run'))
    parser.add_argument('--compiler', action='store_true', help='compile queries instead of interpreting them')
    args = parser.parse_args()
    args.repl, args.jar = args.repl.resolve(), args.jar.resolve()
    workdir = args.workdir.resolve()
    workdir.mkdir(parents=True, exist_ok=True)

    db = sqlite3.connect(':memory:')
    script, queries, expected = [], [], []
    version, snapshot_version, tables = 0, -1, []

    def snapshot():
        """Registers the current tables as sources <table>_v<version>."""
        nonlocal snapshot_version, tables
        tables = [t for (t,) in db.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        for table in tables:
            columns = db.execute(f'PRAGMA table_info({table})').fetchall()
            fields = ', '.join(
                f'{name} {nes_type(ctype)}{" NOT NULL" if (notnull or pk) else ""}' for _, name, ctype, notnull, _, pk in columns)
            data = workdir / f'{table}_v{version}.csv'
            rows = db.execute(f'SELECT * FROM {table} ORDER BY rowid').fetchall()
            data.write_text(''.join(','.join(csv_value(v) for v in row) + '\n' for row in rows))
            script.append(f'CREATE LOGICAL SOURCE {table}_v{version}({fields});')
            script.append(f'CREATE PHYSICAL SOURCE FOR {table}_v{version} TYPE File SET('
                          f"'{data}' AS \"SOURCE\".FILE_PATH, 'CSV' AS INPUT_FORMATTER.\"TYPE\");")
        snapshot_version = version

    for kind, sql in records(args.slt):
        if kind == 'statement':
            try:
                db.execute(sql)
            except sqlite3.Error:
                continue
            if MODIFYING.match(sql):
                version += 1
            continue
        if len(queries) >= args.limit or (args.subqueries_only and not SUBQUERY.search(sql)):
            continue
        try:
            rows = db.execute(sql).fetchall()
        except sqlite3.Error:
            continue  # the file expects an error here
        if snapshot_version != version:
            snapshot()
        versioned = sql
        for table in tables:
            versioned = re.sub(rf'(?i)\b{table}\b', f'{table}_v{version}', versioned)
        n = len(queries)
        queries.append(sql)
        expected.append(sorted(','.join(normalize(v) for v in row) for row in rows))
        script.append(f"CALCITE {versioned} INTO File('{workdir / f'q{n}.csv'}' AS \"SINK\".FILE_PATH, "
                      f"'CSV' AS \"SINK\".OUTPUT_FORMAT, 'localhost:8080' AS \"SINK\".HOST);")
    (workdir / 'queries.sql').write_text('\n'.join(script) + '\n')

    port = free_port()
    planner = subprocess.Popen([args.java, '-jar', str(args.jar), str(port)], stdout=subprocess.PIPE,
                               stderr=open(workdir / 'planner.log', 'w'), text=True)
    started = time.time()
    try:
        planner.stdout.readline()  # "listening on port ..."
        env = {'NES_CALCITE_PLANNER': f'localhost:{port}', 'PATH': '/usr/bin:/bin'}
        mode = 'COMPILER' if args.compiler else 'INTERPRETER'
        with open(workdir / 'queries.sql') as stdin, open(workdir / 'repl.log', 'w') as log:
            subprocess.run([str(args.repl), '-e', 'RECOVER', '--on-exit', 'WAIT_FOR_QUERY_TERMINATION',
                            '--', f'--worker.default_query_execution.execution_mode={mode}',
                            # The tables of the tests end: joins and aggregations without a window run over them.
                            '--optimizer.finite_inputs=true',
                            # Joins of dozens of tables produce rows wider than the default 1 KiB pages.
                            '--worker.default_query_execution.page_size=16384',
                            # Thousands of concurrent queries, some holding 10^6-row windows: 256 MiB is not enough.
                            '--worker.total_memory_in_bytes=8589934592'],
                           stdin=stdin, stdout=log, stderr=subprocess.STDOUT, cwd=workdir, env=env, timeout=7000)
    finally:
        planner.terminate()

    errors = re.findall(r'Error encountered: (.*)', (workdir / 'repl.log').read_text(errors='replace'))
    # A query that fails while running is only logged here; it shows up as a missing or partial result.
    log = workdir / 'nes-repl.log'
    runtime_failures = re.findall(r'Query Failed: (.*)', log.read_text(errors='replace')) if log.exists() else []
    errors += [f'failed at runtime: {failure}' for failure in runtime_failures]
    outcome = Counter()
    for i, rows in enumerate(expected):
        kind = 'subquery' if SUBQUERY.search(queries[i]) else 'other'
        result = workdir / f'q{i}.csv'
        if not result.exists():
            outcome[(kind, 'not executed')] += 1
            continue
        lines = result.read_text().splitlines()
        # The header carries each column's type (NAME:TYPE:NULLABILITY); floats print without decimals (68, not 68.0).
        floats = [':FLOAT' in column for column in lines[0].split(',')] if lines else []
        actual = sorted(
            ','.join(normalize(float(v)) if is_float and v not in ('NULL', '') else normalize(v)
                     for v, is_float in zip(line.split(','), floats + [False] * len(line)))
            for line in lines[1:])
        ok = actual == rows
        outcome[(kind, 'ok' if ok else 'wrong result')] += 1
        outcome[('all', 'ok with rows')] += ok and bool(rows)
        if not ok:
            (workdir / f'q{i}.diff').write_text(f'{queries[i]}\nexpected {rows}\nactual   {actual}\n')
    summary = {f'{kind} {result}': count for (kind, result), count in sorted(outcome.items())}
    print(f'{len(queries)} queries in {time.time() - started:.0f}s: {summary}')
    for reason, count in Counter(re.sub(r'\d+', 'N', e)[:160] for e in errors).most_common(8):
        print(f'  {count:5d} x {reason}')


if __name__ == '__main__':
    main()
