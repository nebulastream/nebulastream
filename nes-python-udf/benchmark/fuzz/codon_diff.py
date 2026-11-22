"""Differential test of Codon INLINE UDFs against CPython.

A case is one Python module with a header that tells the UDF signature in NES types:

    # codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
    # codon-fuzz-rows: 300                          (optional, default --rows)
    # codon-fuzz-inputs: [["abc", 3], ["", -1]]     (optional JSON rows, run before the generated rows)
    # codon-fuzz-field: 0 s.lower()                 (optional labels of the fields of a split result, see below)
    # codon-fuzz-split: 1                           (optional: a VARSIZED result holds fields separated by \\x1f)

The harness calls the entry function on every row in CPython (in a child process) and with nes-python-udf-runner, which
compiles the module as CREATE FUNCTION does and calls the compiled UDF. It compares the results row by row and appends one
JSON line per case to --results. Statuses: ok, diff, codon-reject (Codon did not compile the module), crash (the runner
died on a signal), timeout, reference-error (the module fails in CPython, so the case is not valid).

Usage:
    codon_diff.py CASE.py|DIR [...] --results FILE [--jobs N] [--rows N] [--seed S] [--build DIR]
"""

import argparse
import concurrent.futures
import hashlib
import importlib.util
import json
import math
import os
import re
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPOSITORY = os.path.abspath(os.path.join(HERE, "../../.."))
sys.path.insert(0, HERE)
import values  # noqa: E402

SEPARATOR = "\x1f"
INT_RANGES = {"INT64": (-(2**63), 2**63 - 1), "INT32": (-(2**31), 2**31 - 1)}


# Escapes of the runner's --escaped mode.
def escape(text):
    out = []
    for byte in text.encode("utf-8", "surrogatepass"):
        if byte == 0x5C:
            out.append("\\\\")
        elif byte < 0x20 or byte == 0x7F:
            out.append("\\x%02x" % byte)
        else:
            out.append(chr(byte))
    # The loop works on bytes; join them back as Latin-1 and decode as UTF-8 to keep multibyte sequences.
    return "".join(out).encode("latin-1").decode("utf-8", "replace")


def unescape(text):
    data = bytearray()
    index = 0
    raw = text.encode("utf-8", "surrogateescape")
    while index < len(raw):
        byte = raw[index]
        if byte == 0x5C and index + 1 < len(raw):
            following = chr(raw[index + 1])
            if following == "x":
                data.append(int(raw[index + 2:index + 4], 16))
                index += 4
                continue
            data.append({"t": 9, "n": 10, "r": 13}.get(following, raw[index + 1]))
            index += 2
            continue
        data.append(byte)
        index += 1
    return data.decode("utf-8", "replace")


def format_input(value, kind):
    if value is None:
        return "\\N"
    if kind == "VARSIZED":
        return escape(value)
    if kind == "BOOLEAN":
        return "1" if value else "0"
    if kind == "FLOAT64":
        return repr(value)
    return str(value)


# ---------------------------------------------------------------------------------------------------------------- cases
def parse_case(path):
    header = {"rows": None, "inputs": [], "fields": {}, "split": False}
    with open(path, encoding="utf-8") as file:
        for line in file:
            match = re.match(r"#\s*codon-fuzz(-\w+)?:\s*(.*)$", line.strip())
            if not match:
                continue
            key, value = match.group(1) or "", match.group(2)
            if key == "":
                for part in value.split():
                    name, _, setting = part.partition("=")
                    header[name] = setting
            elif key == "-rows":
                header["rows"] = int(value)
            elif key == "-inputs":
                header["inputs"] += json.loads(value)
            elif key == "-field":
                index, _, label = value.partition(" ")
                header["fields"][int(index)] = label
            elif key == "-split":
                header["split"] = value.strip() not in ("0", "")
    if "entry" not in header or "returns" not in header or "args" not in header:
        raise ValueError(f"{path}: the header needs entry=, args= and returns=")
    header["args"] = header["args"].split(",")
    return header


# ------------------------------------------------------------------------------------------------------------ reference
def run_reference(module_path, entry, types, returns, rows_path, out_path):
    """Child process: calls the entry function in CPython and writes the results as the runner does."""
    spec = importlib.util.spec_from_file_location("fuzzcase", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    function = getattr(module, entry)

    def alarm(*_):
        raise TimeoutError("reference timeout")

    signal.signal(signal.SIGALRM, alarm)
    sys.setrecursionlimit(10000)
    with open(rows_path, encoding="utf-8") as rows_file, open(out_path, "w", encoding="utf-8") as out:
        for line in rows_file:
            row = json.loads(line)
            if any(value is None for value in row):
                # The runner declares the arguments NOT NULL: a NULL argument gives NULL without a call.
                out.write("\\N\n")
                continue
            signal.alarm(5)
            try:
                result = function(*row)
                text = format_result(result, returns)
            except TimeoutError:
                text = "TIMEOUT"
            except RecursionError:
                text = "ERROR RecursionError"
            except Exception as error:  # noqa: BLE001 - every exception is an ERROR row, as in the runner
                text = "ERROR " + escape(f"{type(error).__name__}: {error}")
            finally:
                signal.alarm(0)
            out.write(text + "\n")


def format_result(result, returns):
    if result is None:
        return "\\N"
    if returns == "VARSIZED":
        if not isinstance(result, str):
            return "BADTYPE " + type(result).__name__
        return escape(result)
    if returns == "BOOLEAN":
        return "1" if result else "0"
    if returns == "FLOAT64":
        return repr(float(result))
    if returns in INT_RANGES:
        if isinstance(result, bool) or not isinstance(result, int):
            return "BADTYPE " + type(result).__name__
        low, high = INT_RANGES[returns]
        if not low <= result <= high:
            return "OVERFLOW " + str(result)
        return str(result)
    raise ValueError(returns)


# -------------------------------------------------------------------------------------------------------------- compare
def float_ulps(a, b):
    def ordered(x):
        bits = struct.unpack("<q", struct.pack("<d", x))[0]
        return bits if bits >= 0 else -(2**63) - bits
    return abs(ordered(a) - ordered(b))


def parse_float(text):
    try:
        return float(text)
    except ValueError:
        return None


def classify(reference, codon, returns):
    """None if the two result lines agree, else a short kind of the difference."""
    if reference == codon:
        return None
    if reference.startswith("OVERFLOW"):
        return "known-int64-overflow"
    if reference == "TIMEOUT":
        return "reference-timeout"
    if reference.startswith("ERROR") and codon.startswith("ERROR"):
        return None  # Codon's messages do not carry the CPython exception text.
    if reference.startswith("ERROR"):
        return "cpython-error-codon-value"
    if codon.startswith("ERROR"):
        return "codon-error-cpython-value"
    if reference == "\\N" or codon == "\\N":
        return "null"
    if returns == "FLOAT64":
        a, b = parse_float(reference), parse_float(codon)
        if a is not None and b is not None:
            if (math.isnan(a) and math.isnan(b)) or a == b and math.copysign(1, a) == math.copysign(1, b):
                return None
            if a == b:
                return "float-signed-zero"
            if math.isfinite(a) and math.isfinite(b) and float_ulps(a, b) <= 2:
                return "float-ulp"
        return "float"
    return "value"


def compare_split(reference, codon, labels):
    """Differences per field of a split VARSIZED result (the generated multi-expression cases)."""
    ref_fields = unescape(reference).split(SEPARATOR)
    codon_fields = unescape(codon).split(SEPARATOR)
    if len(ref_fields) != len(codon_fields):
        return [{"field": None, "kind": "field-count", "cpython": reference, "codon": codon}]
    diffs = []
    for index, (a, b) in enumerate(zip(ref_fields, codon_fields)):
        if a == b:
            continue
        kind = "value"
        if a.startswith("!E") and b.startswith("!E"):
            continue
        if a.startswith("!E"):
            kind = "cpython-error-codon-value"
        elif b.startswith("!E"):
            kind = "codon-error-cpython-value"
        else:
            fa, fb = parse_float(a), parse_float(b)
            if fa is not None and fb is not None and re.search(r"[.eEn]", a + b):
                if math.isnan(fa) and math.isnan(fb):
                    continue
                if math.isfinite(fa) and math.isfinite(fb) and fa != fb and float_ulps(fa, fb) <= 2:
                    kind = "float-ulp"
        diffs.append({"field": index, "label": labels.get(index), "kind": kind, "cpython": a, "codon": b})
    return diffs


# ------------------------------------------------------------------------------------------------------------------ run
def run_case(path, options):
    started = time.time()
    header = parse_case(path)
    types, returns = header["args"], header["returns"]
    count = header["rows"] or options.rows
    seed = int(hashlib.sha256(f"{options.seed}:{os.path.basename(path)}".encode()).hexdigest()[:8], 16)
    rows = [list(r) for r in header["inputs"]] + values.rows(types, count, seed, options.null_fraction)
    record = {"case": os.path.relpath(path, REPOSITORY), "rows": len(rows), "args": types, "returns": returns}

    work = tempfile.mkdtemp(prefix="codon-diff-", dir=options.work)
    try:
        module = "fc_" + re.sub(r"\W", "_", os.path.splitext(os.path.basename(path))[0])
        module_path = os.path.join(work, module + ".py")
        shutil.copy(path, module_path)
        rows_json = os.path.join(work, "rows.jsonl")
        with open(rows_json, "w", encoding="utf-8") as file:
            for row in rows:
                file.write(json.dumps(row, ensure_ascii=False) + "\n")
        rows_tsv = os.path.join(work, "rows.tsv")
        with open(rows_tsv, "w", encoding="utf-8") as file:
            for row in rows:
                file.write("\t".join(format_input(v, t) for v, t in zip(row, types)) + "\n")

        ref_out = os.path.join(work, "cpython.out")
        reference = subprocess.run(
            [sys.executable, "-I", os.path.abspath(__file__), "--reference", module_path, header["entry"], ",".join(types),
             returns, rows_json, ref_out], capture_output=True, text=True, timeout=options.timeout)
        if reference.returncode != 0:
            record.update(status="reference-error", detail=reference.stderr[-2000:])
            return record

        codon_out = os.path.join(work, "codon.out")
        command = [options.runner, "--compiler", "Codon", "--entrypoint", f"{module}.{header['entry']}", "--import-path", work,
                   "--returns", returns, "--input", rows_tsv, "--rounds", "0", "--escaped", "1", "--output", codon_out]
        for index, kind in enumerate(types):
            command += ["--arg", f"column:{index}:{kind}"]
        try:
            codon = subprocess.run(command, capture_output=True, text=True, timeout=options.timeout)
        except subprocess.TimeoutExpired:
            record.update(status="timeout")
            return record
        if codon.returncode < 0:
            done = 0
            if os.path.exists(codon_out):
                with open(codon_out, encoding="utf-8", errors="replace") as file:
                    done = file.read().count("\n")
            with open(ref_out, encoding="utf-8") as file:
                ref_lines = file.read().split("\n")
            record.update(status="crash", signal=-codon.returncode, detail=codon.stderr[-2000:], command=command,
                          crash_row=done, crash_inputs=rows[done] if done < len(rows) else None,
                          cpython=ref_lines[done] if done < len(ref_lines) else None)
            keep = os.path.join(options.keep, os.path.basename(work))
            shutil.copytree(work, keep)
            record["kept"] = keep
            return record
        if codon.returncode not in (0, 2) or not os.path.exists(codon_out):
            record.update(status="codon-reject", detail=(codon.stderr or codon.stdout)[-2000:])
            return record
        try:
            record["runner"] = json.loads(codon.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            pass

        with open(ref_out, encoding="utf-8") as file:
            ref_lines = file.read().split("\n")[:len(rows)]
        with open(codon_out, encoding="utf-8", errors="surrogateescape") as file:
            codon_lines = file.read().split("\n")[:len(rows)]
        diffs = []
        for index, (a, b) in enumerate(zip(ref_lines, codon_lines)):
            if header["split"] and not a.startswith(("ERROR", "\\N", "TIMEOUT")) and not b.startswith(("ERROR", "\\N")):
                found = compare_split(a, b, header["fields"])
            else:
                kind = classify(a, b, returns)
                found = [] if kind is None else [{"field": None, "kind": kind, "cpython": a, "codon": b}]
            for diff in found:
                diff.update(row=index, inputs=rows[index])
                diffs.append(diff)
        kinds = {}
        for diff in diffs:
            kinds[diff["kind"]] = kinds.get(diff["kind"], 0) + 1
        real = [d for d in diffs if not d["kind"].startswith(("known-", "reference-"))]
        record.update(status="diff" if real else "ok", kinds=kinds, diffs=diffs[:options.max_diffs])
        return record
    finally:
        record["seconds"] = round(time.time() - started, 1)
        if record.get("status") != "crash":
            shutil.rmtree(work, ignore_errors=True)


def collect(paths):
    cases = []
    for path in paths:
        if os.path.isdir(path):
            cases += sorted(os.path.join(path, name) for name in os.listdir(path) if name.endswith(".py"))
        else:
            cases.append(path)
    return cases


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--reference":
        run_reference(sys.argv[2], sys.argv[3], sys.argv[4].split(","), sys.argv[5], sys.argv[6], sys.argv[7])
        return
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("cases", nargs="+")
    parser.add_argument("--results", required=True)
    parser.add_argument("--jobs", type=int, default=8)
    parser.add_argument("--rows", type=int, default=300)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--null-fraction", type=float, default=0.0)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--max-diffs", type=int, default=50)
    parser.add_argument("--build", default=os.path.join(REPOSITORY, "cmake-build-relwithdebinfo-libstdcxx-none"))
    parser.add_argument("--work", default=None, help="directory for the temporary files of each case")
    parser.add_argument("--keep", default=None, help="directory that keeps the files of crashed cases")
    options = parser.parse_args()
    options.runner = os.path.join(options.build, "nes-python-udf/benchmark/runner/nes-python-udf-runner")
    options.keep = options.keep or os.path.join(os.path.dirname(os.path.abspath(options.results)), "crashes")
    os.makedirs(options.keep, exist_ok=True)

    cases = collect(options.cases)
    totals = {}
    with open(options.results, "a", encoding="utf-8") as results, \
            concurrent.futures.ThreadPoolExecutor(options.jobs) as pool:
        futures = {pool.submit(run_case, case, options): case for case in cases}
        for future in concurrent.futures.as_completed(futures):
            try:
                record = future.result()
            except Exception as error:  # noqa: BLE001 - a harness error of one case does not stop the run
                record = {"case": futures[future], "status": "harness-error", "detail": repr(error)}
            results.write(json.dumps(record, ensure_ascii=False) + "\n")
            results.flush()
            totals[record["status"]] = totals.get(record["status"], 0) + 1
            extra = f" {record.get('kinds')}" if record.get("kinds") else ""
            print(f"{record['status']:16} {record['case']}{extra}", flush=True)
    print(json.dumps(totals))


if __name__ == "__main__":
    main()
