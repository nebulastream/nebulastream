"""Differential test of Codon INLINE UDFs against CPython.

A case is one Python module with a header that tells the UDF signature in NES types:

    # codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
    # codon-fuzz-rows: 300                          (optional, default --rows)
    # codon-fuzz-inputs: [["abc", 3], ["", -1]]     (optional JSON rows, run before the generated rows)
    # codon-fuzz-field: 0 s.lower()                 (optional labels of the fields of a split result, see below)
    # codon-fuzz-split: 1                           (optional: a VARSIZED result holds fields separated by \\x1f)

The harness calls the entry function on every row in CPython (in a child process) and in the target (--compiler):
Codon or Numba with nes-python-udf-runner, which compiles the module as CREATE FUNCTION does; Babelfish with
FuzzCaseRunner of the Babelfish port (GraalPy in the Babelfish query engine); LPython with lpython_driver.c (only cases
whose functions LPython can type: the harness annotates the entry function from the header). It compares the results row
by row and appends one JSON line per case to --results. Statuses: ok, diff, reject (the target did not compile or run the
module), crash (the target died on a signal), timeout, reference-error (the module fails in CPython, so the case is not
valid).

Usage:
    codon_diff.py CASE.py|DIR [...] --results FILE [--compiler Codon|Numba|Babelfish|LPython] [--jobs N] [--rows N] [--seed S] [--build DIR]
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
        return "cpython-error-target-value"
    if codon.startswith("ERROR"):
        return "target-error-cpython-value"
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
        return [{"field": None, "kind": "field-count", "cpython": reference, "target": codon}]
    diffs = []
    for index, (a, b) in enumerate(zip(ref_fields, codon_fields)):
        if a == b:
            continue
        kind = "value"
        if a.startswith("!E") and b.startswith("!E"):
            continue
        if a.startswith("!E"):
            kind = "cpython-error-target-value"
        elif b.startswith("!E"):
            kind = "target-error-cpython-value"
        else:
            fa, fb = parse_float(a), parse_float(b)
            if fa is not None and fb is not None and re.search(r"[.eEn]", a + b):
                if math.isnan(fa) and math.isnan(fb):
                    continue
                if math.isfinite(fa) and math.isfinite(fb) and fa != fb and float_ulps(fa, fb) <= 2:
                    kind = "float-ulp"
        diffs.append({"field": index, "label": labels.get(index), "kind": kind, "cpython": a, "target": b})
    return diffs


# -------------------------------------------------------------------------------------------------------------- targets
# A target runs the case on the rows of rows_tsv and writes one escaped result line per row to out_path. It returns
# (CompletedProcess, command), or (None, reason) if it cannot run the case at all.
def run_nes(options, header, module, work, rows_tsv, out_path):
    command = [options.runner, "--compiler", options.compiler, "--entrypoint", f"{module}.{header['entry']}", "--import-path",
               work, "--returns", header["returns"], "--input", rows_tsv, "--rounds", "0", "--escaped", "1", "--output", out_path]
    for index, kind in enumerate(header["args"]):
        command += ["--arg", f"column:{index}:{kind}"]
    environment = dict(os.environ)
    if options.compiler == "Numba":
        environment["NES_NUMBA_PYTHON"] = options.numba_python
    return subprocess.run(command, capture_output=True, text=True, timeout=options.timeout, env=environment), command


BABELFISH_WRAPPER = r'''

def __fz_escape(text):
    out = []
    for byte in text.encode("utf-8", "surrogatepass"):
        if byte == 0x5C:
            out.append("\\\\")
        elif byte < 0x20 or byte == 0x7F:
            out.append("\\x%02x" % byte)
        else:
            out.append(chr(byte))
    return "".join(out).encode("latin-1").decode("utf-8", "replace")


def __fz_format(result):
    if result is None:
        return "\\N"
    kind = "RETURNS"
    if kind == "VARSIZED":
        return __fz_escape(result) if isinstance(result, str) else "BADTYPE " + type(result).__name__
    if kind == "BOOLEAN":
        return "1" if result else "0"
    if kind == "FLOAT64":
        return repr(float(result))
    if isinstance(result, bool) or not isinstance(result, int):
        return "BADTYPE " + type(result).__name__
    if not -(2**63) <= result < 2**63:
        return "OVERFLOW " + str(result)
    return str(result)


__fz_entry = ENTRY


# Babelfish calls the module's global function udf(rec, ctx), so the wrapper takes that name.
def udf(rec, ctx):
    try:
        return __fz_format(__fz_entry(ARGUMENTS))
    except BaseException as error:
        return "ERROR " + __fz_escape(type(error).__name__ + ": " + str(error))


lambda rec, ctx: udf(rec, ctx)
'''


def run_babelfish(options, header, module, work, rows_tsv, out_path):
    names, counts = [], {"s": 0, "n": 0, "x": 0, "b": 0}
    for kind in header["args"]:
        prefix, convert = {"VARSIZED": ("s", "str"), "INT64": ("n", "int"), "INT32": ("n", "int"), "FLOAT64": ("x", "float"),
                           "BOOLEAN": ("b", "bool")}[kind]
        names.append(f"{convert}(rec.{prefix}{counts[prefix]})")
        counts[prefix] += 1
    if counts["s"] > 3 or counts["n"] > 2 or counts["x"] > 2 or counts["b"] > 1:
        return None, "signature does not fit FuzzCaseRunner.FuzzRow"
    with open(os.path.join(work, module + ".py"), encoding="utf-8") as file:
        source = file.read()
    code = source + BABELFISH_WRAPPER.replace("RETURNS", header["returns"]).replace("ENTRY", header["entry"]).replace(
        "ARGUMENTS", ", ".join(names))
    code_path = os.path.join(work, "babelfish_udf.py")
    with open(code_path, "w", encoding="utf-8") as file:
        file.write(code)
    babelfish = options.babelfish
    jars = [p for p in open(os.path.join(babelfish, "benchmark/target/classpath.txt")).read().split(":") if p.endswith(".jar")]
    classpath = ":".join([os.path.join(babelfish, "benchmark/target/classes"), *jars])
    command = [options.java, "--sun-misc-unsafe-memory-access=allow", "--add-opens=java.base/java.nio=ALL-UNNAMED",
               "--enable-native-access=ALL-UNNAMED", "-cp", classpath, "de.tub.dima.babelfish.port.FuzzCaseRunner", code_path,
               ",".join(header["args"]), rows_tsv, out_path]
    return subprocess.run(command, capture_output=True, text=True, timeout=options.timeout, cwd=work), command


LPYTHON_TYPES = {"VARSIZED": ("str", "s"), "INT64": ("i64", "i"), "INT32": ("i64", "i"), "FLOAT64": ("f64", "f"),
                 "BOOLEAN": ("bool", "b")}


def run_lpython(options, header, module, work, rows_tsv, out_path):
    """Annotates the entry function from the header, adds a @ccallable entry, and runs it with lpython_driver.c."""
    with open(os.path.join(work, module + ".py"), encoding="utf-8") as file:
        source = file.read()
    entry = header["entry"]
    match = re.search(rf"^def {entry}\(([^)]*)\):", source, re.MULTILINE)
    if not match:
        return None, "the entry function has no plain def line"
    names = [name.strip() for name in match.group(1).split(",") if name.strip()]
    if len(names) != len(header["args"]) or any(":" in name for name in names):
        return None, "the entry parameters do not match the header"
    kinds = [LPYTHON_TYPES[kind] for kind in header["args"]]
    returns, returns_letter = LPYTHON_TYPES[header["returns"]]
    parameters = ", ".join(f"{name}: {kind}" for name, (kind, _) in zip(names, kinds))
    source = source[:match.start()] + f"def {entry}({parameters}) -> {returns}:" + source[match.end():]
    # LPython miscompiles a @ccallable function that passes its str argument on, so the entry copies them (E80).
    copies = "".join(f"    c{i}: {kind} = p{i}\n" for i, (kind, _) in enumerate(kinds))
    call = f"{entry}({', '.join(f'c{i}' for i in range(len(kinds)))})"
    entry_returns = "i32" if returns == "bool" else returns
    body = f"    if {call}:\n        return i32(1)\n    return i32(0)\n" if returns == "bool" else f"    return {call}\n"
    signature = ", ".join(f"p{i}: {kind}" for i, (kind, _) in enumerate(kinds))
    program = ("from lpython import i64, f64, i32, ccallable\n" + source +
               f"\n\n@ccallable\ndef udf_entry({signature}) -> {entry_returns}:\n{copies}{body}")
    program_path = os.path.join(work, "lpython_udf.py")
    with open(program_path, "w", encoding="utf-8") as file:
        file.write(program)
    obj = os.path.join(work, "udf.o")
    compiled = subprocess.run([options.lpython, "--fast", "-c", program_path, "-o", obj], capture_output=True, text=True,
                              timeout=options.timeout, cwd=work)
    if compiled.returncode != 0:
        return None, (compiled.stdout + compiled.stderr)[-2000:]
    subprocess.run(["objcopy", "--redefine-sym", "main=lpython_main", obj], check=True)
    runtime = subprocess.run([options.lpython, "--get-rtl-dir"], capture_output=True, text=True).stdout.strip()
    driver = os.path.join(work, "driver")
    linked = subprocess.run(["cc", "-O1", "-o", driver, os.path.join(HERE, "lpython_driver.c"), obj, f"-L{runtime}",
                             "-llpython_runtime_static", "-lm"], capture_output=True, text=True)
    if linked.returncode != 0:
        return None, "link: " + linked.stderr[-2000:]
    with open(rows_tsv, encoding="utf-8") as file:
        count = sum(1 for _ in file)
    letters = "".join(letter for _, letter in kinds)
    command = [driver, letters, returns_letter, rows_tsv, "0", out_path]
    first, aborted, stderr = 0, 0, ""
    open(out_path, "w").close()
    while first < count:
        command[4] = str(first)
        completed = subprocess.run(command, capture_output=True, text=True, timeout=options.timeout)
        with open(out_path, encoding="utf-8", errors="replace") as file:
            done = file.read().count("\n")
        if completed.returncode == 0 and done >= count:
            break
        # The process ended at row `done`: LPython's runtime error (or a fault, as --fast has no checks) for that row.
        aborted += 1
        stderr = completed.stderr[-300:]
        with open(out_path, "a", encoding="utf-8") as file:
            reason = f"signal {-completed.returncode}" if completed.returncode < 0 else f"exit {completed.returncode}"
            file.write("ERROR " + escape(f"LPython {reason}: {completed.stderr.strip()[-120:]}") + "\n")
        first = done + 1
    result = subprocess.CompletedProcess(command, 0, json.dumps({"aborted_rows": aborted}), stderr)
    return result, command


TARGETS = {"Codon": run_nes, "Numba": run_nes, "Babelfish": run_babelfish, "LPython": run_lpython}


# ------------------------------------------------------------------------------------------------------------------ run
def run_case(path, options):
    started = time.time()
    header = parse_case(path)
    types, returns = header["args"], header["returns"]
    count = header["rows"] or options.rows
    seed = int(hashlib.sha256(f"{options.seed}:{os.path.basename(path)}".encode()).hexdigest()[:8], 16)
    rows = [list(r) for r in header["inputs"]] + values.rows(types, count, seed, options.null_fraction)
    record = {"case": os.path.relpath(path, REPOSITORY), "compiler": options.compiler, "rows": len(rows), "args": types,
              "returns": returns}

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

        target_out = os.path.join(work, "target.out")
        try:
            completed, command = TARGETS[options.compiler](options, header, module, work, rows_tsv, target_out)
        except subprocess.TimeoutExpired:
            record.update(status="timeout")
            return record
        if completed is None:
            record.update(status="reject", detail=command)
            return record
        if completed.returncode < 0 or completed.returncode in (134, 139):
            done = 0
            if os.path.exists(target_out):
                with open(target_out, encoding="utf-8", errors="replace") as file:
                    done = file.read().count("\n")
            with open(ref_out, encoding="utf-8") as file:
                ref_lines = file.read().split("\n")
            record.update(status="crash", signal=-completed.returncode if completed.returncode < 0 else completed.returncode,
                          detail=completed.stderr[-2000:], command=command, crash_row=done,
                          crash_inputs=rows[done] if done < len(rows) else None,
                          cpython=ref_lines[done] if done < len(ref_lines) else None)
            keep = os.path.join(options.keep, os.path.basename(work))
            shutil.copytree(work, keep)
            record["kept"] = keep
            return record
        if completed.returncode not in (0, 2) or not os.path.exists(target_out):
            record.update(status="reject", detail=(completed.stderr or completed.stdout)[-2000:])
            return record
        try:
            record["runner"] = json.loads(completed.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            pass

        with open(ref_out, encoding="utf-8") as file:
            ref_lines = file.read().split("\n")[:len(rows)]
        with open(target_out, encoding="utf-8", errors="surrogateescape") as file:
            codon_lines = file.read().split("\n")[:len(rows)]
        if len(codon_lines) < len(rows):
            record.update(status="reject", detail=f"{len(codon_lines)} of {len(rows)} result lines")
            return record
        diffs = []
        for index, (a, b) in enumerate(zip(ref_lines, codon_lines)):
            if header["split"] and not a.startswith(("ERROR", "\\N", "TIMEOUT")) and not b.startswith(("ERROR", "\\N")):
                found = compare_split(a, b, header["fields"])
            else:
                kind = classify(a, b, returns)
                found = [] if kind is None else [{"field": None, "kind": kind, "cpython": a, "target": b}]
            for diff in found:
                diff.update(row=index, inputs=rows[index])
                if options.compiler == "Babelfish" and any(isinstance(v, str) and "\x00" in v for v in rows[index]):
                    diff["kind"] = "known-babelfish-text-ends-at-nul"
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
    parser.add_argument("--compiler", default="Codon", choices=sorted(TARGETS))
    parser.add_argument("--numba-python", default=os.environ.get("NES_NUMBA_PYTHON"), help="Python with Numba (Numba)")
    parser.add_argument("--babelfish", default="/data-ssd/lukas/babelfish", help="Babelfish checkout (Babelfish)")
    parser.add_argument("--java", default="/home/lukas-ldap.linux/tools/graalvm/bin/java", help="GraalVM 25 java (Babelfish)")
    parser.add_argument("--lpython", default=None, help="the lpython binary (LPython)")
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
            results.write(json.dumps(record) + "\n")  # ASCII: target output can hold bytes that are not UTF-8
            results.flush()
            totals[record["status"]] = totals.get(record["status"], 0) + 1
            extra = f" {record.get('kinds')}" if record.get("kinds") else ""
            print(f"{record['status']:16} {record['case']}{extra}", flush=True)
    print(json.dumps(totals))


if __name__ == "__main__":
    main()
