"""Runs the sample rows of real-UDF case modules in this Python and prints each result as codon_diff.py formats it.

A case module is the UDF's code with a header that codon_diff.py reads (`# codon-fuzz: entry=... args=... returns=...`,
`# codon-fuzz-rows: 0`, `# codon-fuzz-inputs: [[...], ...]`) and a `# real-udf:` line with its provenance. The checker
reports a case as BROKEN if it does not import, if the header is invalid, or if every row raises.

Usage: python3 -I check_case.py CASE.py [...]   (exit code 1 if any case is BROKEN)
"""

import importlib.util
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../fuzz"))
import codon_diff  # noqa: E402

TYPES = {"VARSIZED": str, "INT64": int, "INT32": int, "FLOAT64": float, "BOOLEAN": bool}


def check(path):
    try:
        header = codon_diff.parse_case(path)
    except Exception as error:  # noqa: BLE001
        return f"BROKEN header: {error}"
    for kind in header["args"] + [header["returns"]]:
        if kind not in TYPES:
            return f"BROKEN header: unsupported type {kind}"
    if not header["inputs"]:
        return "BROKEN header: no codon-fuzz-inputs"
    try:
        spec = importlib.util.spec_from_file_location("realudf_case", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        function = getattr(module, header["entry"])
    except Exception as error:  # noqa: BLE001
        return f"BROKEN import: {type(error).__name__}: {error}"
    lines, errors = [], 0
    for row in header["inputs"]:
        if len(row) != len(header["args"]):
            return f"BROKEN inputs: row {row!r} has {len(row)} values for {len(header['args'])} arguments"
        for value, kind in zip(row, header["args"]):
            if value is not None and not isinstance(value, TYPES[kind]) and not (kind == "FLOAT64" and isinstance(value, int)):
                return f"BROKEN inputs: {value!r} is not {kind}"
        try:
            text = codon_diff.format_result(function(*row), header["returns"])
        except Exception as error:  # noqa: BLE001
            text = f"ERROR {type(error).__name__}: {error}"
        errors += text.startswith(("ERROR", "BADTYPE", "OVERFLOW"))
        lines.append(f"  {row!r} -> {text}")
    status = f"BROKEN {errors} of {len(lines)} rows fail" if errors else f"OK {len(lines)} rows"
    return status + "\n" + "\n".join(lines)


def main():
    broken = False
    for path in sys.argv[1:]:
        result = check(path)
        broken |= result.startswith("BROKEN")
        print(f"{path}: {result}")
    sys.exit(1 if broken else 0)


if __name__ == "__main__":
    main()
