#!/usr/bin/env python3
"""Generate a deterministic ORU_LTE HL7 v2 testdata file matching the minimal
19-column schema expected by the HL7 input formatter.

Schema (flat, one column per top-level HL7 field):
  MSH.1, MSH.2, MSH.3, MSH.4, MSH.5, MSH.6, MSH.7,
  PID.1, PID.2,
  PV1.1, PV1.2,
  ORC.1, ORC.2,
  OBR.1, OBR.2,
  OBX.1, OBX.2,
  NTE.1, NTE.2

The parser uses '\\nMSH' as the message (tuple) delimiter and expects the
stream to end with that sentinel. This script therefore appends a literal
'\\nMSH' at the end of the file.

Usage:
  python3 gen_oru_lte.py <output.csv> <target_bytes>
"""
import argparse
import sys


FACILITIES = ["siteA", "siteB", "siteC", "siteD", "siteE"]
RECEIVERS = ["hospital1", "hospital2", "hospital3"]
PATIENTS = ["alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi"]
LOCATIONS = ["ward_A", "ward_B", "ICU", "ER", "clinic_7"]
TESTS = ["glucose", "hemoglobin", "potassium", "sodium", "creatinine"]
NOTES = ["normal", "elevated", "low", "followup", "borderline"]


def build_message(i: int) -> str:
    sender = FACILITIES[i % len(FACILITIES)]
    receiver = RECEIVERS[i % len(RECEIVERS)]
    patient = PATIENTS[i % len(PATIENTS)]
    location = LOCATIONS[i % len(LOCATIONS)]
    test = TESTS[i % len(TESTS)]
    note = NOTES[i % len(NOTES)]
    # OBX.1 and NTE.1 must parse as UINT64 (see test schema).
    obx_val = (i * 7) % 1000
    nte_val = (i * 3) % 1000
    return "\n".join([
        f"MSH|^~\\&|{sender}|{receiver}|2026{(i % 12) + 1:02d}{(i % 28) + 1:02d}|ORU^LTE^ORU_LTE|m{i}",
        f"PID|p{i}|{patient}",
        f"PV1|v{i}|{location}",
        f"ORC|NW|ord{i}",
        f"OBR|o{i}|{test}",
        f"OBX|{obx_val}|value_{i}",
        f"NTE|{nte_val}|{note}_{i}",
    ])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("output", help="Output file path")
    ap.add_argument("target_bytes", type=int, help="Approximate output size in bytes (before sentinel)")
    args = ap.parse_args()

    sentinel = "\nMSH"
    budget = args.target_bytes - len(sentinel)
    written = 0
    count = 0

    with open(args.output, "w", encoding="ascii") as f:
        while written < budget:
            message = build_message(count)
            # Each message is followed by a segment-delimiter newline so the next message's
            # leading 'MSH' is preceded by '\n' (forming the '\nMSH' tuple delimiter).
            chunk = message + "\n"
            if written + len(chunk) > budget and count > 0:
                break
            f.write(chunk)
            written += len(chunk)
            count += 1
        # Append sentinel so the file ends with the 4-byte tuple delimiter.
        f.write("MSH")

    final_size = written + len("MSH")
    print(f"Wrote {count} messages, {final_size} bytes to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
