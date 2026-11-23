"""Writes a systest that runs real-UDF case modules as EXECUTION OUT_OF_PROCESS (or another execution) UDFs.

The cases live in nes-systests/udf/pyudfs/real_udfs/<origin>/<id>.py, where the systest finds them by ENTRYPOINT
'real_udfs.<origin>.<id>.<entry>'. Each case's header (`# codon-fuzz: entry= args= returns=`, `# codon-fuzz-inputs:`) gives
its signature and sample rows. For each case, this script computes the expected results by calling the entry function in
this Python (run it with the Python of the UDF processes), and writes one query: a logical source with the rows as
tab-separated data, one CREATE FUNCTION, and `SELECT id, <udf>(...)`. The systest parser cuts every line at a '#', so the
rows of a case with a '#' go into nes-systests/testdata/small/real_udfs/<name>.tsv (ATTACH FILE) instead of INLINE data,
and a row whose result holds a '#' is left out. A row whose text holds a tab, a newline or a carriage return is left out; a case that needs packages this Python lacks
(`# real-udf: requires=`), that does not import, or whose rows raise is left out and listed on stderr.

Usage: python3 -I gen_systest.py --out TEST [--execution OUT_OF_PROCESS] [--origins a,b] [--name NAME]
"""

import argparse
import importlib
import importlib.util
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPOSITORY = os.path.abspath(os.path.join(HERE, "../../.."))
PYUDFS = os.path.join(REPOSITORY, "nes-systests/udf/pyudfs")
DATA = os.path.join(REPOSITORY, "nes-systests/testdata/small/real_udfs")
CASES = os.path.join(PYUDFS, "real_udfs")
sys.path.insert(0, os.path.join(HERE, "../fuzz"))
import codon_diff  # noqa: E402


def requires(path):
    for line in open(path, encoding="utf-8"):
        match = re.match(r"#\s*real-udf:.*\brequires=(\S*)", line)
        if match:
            return [p for p in match.group(1).split(",") if p]
    return []


def nes_value(value, kind):
    if kind == "BOOLEAN":
        return "1" if value else "0"
    if kind == "FLOAT64":
        return repr(float(value))
    return str(value)


def nes_result(value, kind):
    """A result as the file sink writes it: a float with 6 decimals and its trailing zeros removed (one kept)."""
    if kind == "FLOAT64":
        text = f"{float(value):.6f}".rstrip("0")
        return text + "0" if text.endswith(".") else text
    return nes_value(value, kind)


def case_query(index, origin, name, path, execution):
    header = codon_diff.parse_case(path)
    missing = [p for p in requires(path) if importlib.util.find_spec(p) is None]
    if missing:
        return None, f"needs {','.join(missing)}"
    sys.path.insert(0, PYUDFS)
    try:
        module = importlib.import_module(f"real_udfs.{origin}.{name}")
        function = getattr(module, header["entry"])
    except Exception as error:  # noqa: BLE001
        return None, f"import: {type(error).__name__}: {error}"
    finally:
        sys.path.remove(PYUDFS)
    rows, results = [], []
    for row in header["inputs"]:
        if any(isinstance(v, str) and re.search(r"[\t\n\r]", v) for v in row):
            continue
        try:
            result = function(*row)
        except Exception as error:  # noqa: BLE001
            return None, f"row {row!r} raises {type(error).__name__}: {error}"
        text = "NULL" if result is None else nes_result(result, header["returns"])
        if re.search(r"[\t\n\r#]", text):
            continue
        rows.append(row)
        results.append(text)
    if not rows:
        return None, "no row without tab or newline"
    columns = ", ".join(f"a{i} {kind} NOT NULL" for i, kind in enumerate(header["args"]))
    source = f"real_{index}"
    udf = f"real_udf_{index}"
    data = ["\t".join([str(i)] + [nes_value(v, k) for v, k in zip(row, header["args"])]) for i, row in enumerate(rows)]
    lines = [f"# {origin}/{name}",
             f"CREATE LOGICAL SOURCE {source}(id INT64 NOT NULL, {columns});",
             f"CREATE PHYSICAL SOURCE FOR {source} TYPE File SET('\\t' AS INPUT_FORMATTER.FIELD_DELIMITER, "
             f"false AS INPUT_FORMATTER.ALLOW_COMMAS_IN_STRINGS);"]
    if any("#" in line for line in data):
        os.makedirs(DATA, exist_ok=True)
        with open(os.path.join(DATA, f"{origin}__{name}.tsv"), "w", encoding="utf-8") as file:
            file.write("\n".join(data) + "\n")
        lines += [f"ATTACH FILE small/real_udfs/{origin}__{name}.tsv"]
    else:
        lines += ["ATTACH INLINE"] + data
    arguments = ", ".join(f"a{i} {kind} NOT NULL" for i, kind in enumerate(header["args"]))
    lines += ["", f"CREATE FUNCTION {udf} ({arguments}) RETURNS {header['returns']} EXECUTION {execution} "
              f"ENTRYPOINT 'real_udfs.{origin}.{name}.{header['entry']}';", "",
              f"SELECT id, {udf}({', '.join(f'a{i}' for i in range(len(header['args'])))}) AS r FROM {source} INTO FILE();",
              "----"]
    lines += [f"{i},{text}" for i, text in enumerate(results)]
    return "\n".join(lines), None


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True)
    parser.add_argument("--execution", default="OUT_OF_PROCESS")
    parser.add_argument("--origins", default=None)
    parser.add_argument("--name", default="udf/RealUdfsOutOfProcess.test")
    arguments = parser.parse_args()
    origins = arguments.origins.split(",") if arguments.origins else sorted(
        d for d in os.listdir(CASES) if os.path.isdir(os.path.join(CASES, d)) and not d.startswith("_"))
    queries, left_out = [], []
    for origin in origins:
        for file in sorted(os.listdir(os.path.join(CASES, origin))):
            if not file.endswith(".py") or file.startswith("_"):
                continue
            query, reason = case_query(len(queries), origin, file[:-3], os.path.join(CASES, origin, file), arguments.execution)
            if query is None:
                left_out.append(f"{origin}/{file[:-3]}: {reason}")
            else:
                queries.append(query)
    with open(arguments.out, "w", encoding="utf-8") as out:
        out.write(f"# name: {arguments.name}\n")
        out.write(f"# description: Real Python UDFs from papers, open-source projects, database and streaming systems "
                  f"(nes-python-udf/benchmark/real-udfs) as EXECUTION {arguments.execution} UDFs, with the results of CPython. "
                  f"Generated by nes-python-udf/benchmark/real-udfs/gen_systest.py.\n")
        out.write("# groups: [PythonUdf, PythonUdfProcess, RealUdfs]\n\n")
        out.write("\n\n".join(queries) + "\n")
    print(f"{len(queries)} queries, {len(left_out)} cases left out", file=sys.stderr)
    for line in left_out:
        print("  " + line, file=sys.stderr)


if __name__ == "__main__":
    main()
