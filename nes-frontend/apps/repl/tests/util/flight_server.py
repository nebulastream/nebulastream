#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow>=15.0"]
# ///
"""
Minimal Arrow Flight server that receives DoPut streams and validates data.

Usage:
    ./flight_server.py [--port PORT] [--timeout SECONDS]

Prints FLIGHT_PORT=<port> on startup, JSON summary on shutdown.
Validates non-nullable columns against expected generator sequences.
Nullable columns are checked for presence but values are expected to be
corrupt (known issue: inline null byte breaks Arrow zero-copy wrapping).
"""

import argparse
import json
import signal
import threading

import pyarrow.flight as flight


class RecordingServer(flight.FlightServerBase):
    def __init__(self, location: str, **kwargs):
        super().__init__(location, **kwargs)
        self._streams: list[dict] = []
        self._lock = threading.Lock()

    def do_put(self, context, descriptor, reader, writer):
        total_rows = 0
        num_batches = 0
        schema = None
        columns: dict[str, list] = {}

        while True:
            try:
                batch, _ = reader.read_chunk()
                if schema is None:
                    schema = batch.schema
                    for f in schema:
                        columns[f.name] = []
                num_batches += 1
                total_rows += batch.num_rows
                for f in schema:
                    columns[f.name].extend(batch.column(f.name).to_pylist())
            except StopIteration:
                break

        validations = self._validate(columns, schema) if columns else {}

        with self._lock:
            if descriptor.descriptor_type == flight.DescriptorType.PATH:
                desc = [p.decode("utf-8") if isinstance(p, bytes) else str(p) for p in descriptor.path]
            else:
                cmd = descriptor.command
                desc = cmd.decode("utf-8") if isinstance(cmd, bytes) else str(cmd)

            schema_info = [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in schema] if schema else []
            self._streams.append({
                "descriptor": desc,
                "rows": total_rows,
                "batches": num_batches,
                "schema": schema_info,
                "validations": validations,
            })

    @staticmethod
    def _validate(columns: dict[str, list], schema) -> dict:
        results = {}

        ids = columns.get("SENSOR$ID", [])
        if not ids:
            results["error"] = "no id column"
            return results

        # 1. ID column: no duplicates, all satisfy filter (id/1000)%2==0
        unique_ids = set(ids)
        bad_filter = [i for i in ids if (i // 1000) % 2 != 0]
        results["id_count"] = len(ids)
        results["id_unique"] = len(unique_ids)
        results["id_no_duplicates"] = len(unique_ids) == len(ids)
        results["id_filter_valid"] = len(bad_filter) == 0
        if bad_filter:
            results["id_bad_filter_sample"] = bad_filter[:5]

        # 2. Non-nullable columns: verify values match expected sequences
        #    vi32 == id, vi8 == id % 101 (wraps 0..100), vu16 == id % 60001,
        #    vf32 ≈ id, vf64 ≈ id * 0.5
        non_nullable_checks = {}

        vi32 = columns.get("SENSOR$VI32", [])
        if vi32:
            m = sum(1 for a, b in zip(ids, vi32) if int(a) % (2**31) != int(b) % (2**31))
            non_nullable_checks["vi32_mismatches"] = m

        vi8 = columns.get("SENSOR$VI8", [])
        if vi8:
            m = sum(1 for a, b in zip(ids, vi8) if int(a) % 101 != ((int(b) % 101) + 101) % 101)
            non_nullable_checks["vi8_mismatches"] = m

        vu16 = columns.get("SENSOR$VU16", [])
        if vu16:
            m = sum(1 for a, b in zip(ids, vu16) if int(a) % 60001 != int(b))
            non_nullable_checks["vu16_mismatches"] = m

        vf32 = columns.get("SENSOR$VF32", [])
        if vf32:
            m = sum(1 for a, b in zip(ids, vf32) if abs(float(a) - float(b)) > 1.0)
            non_nullable_checks["vf32_mismatches"] = m

        vf64 = columns.get("SENSOR$VF64", [])
        if vf64:
            m = sum(1 for a, b in zip(ids, vf64) if abs(float(a) * 0.5 - float(b)) > 0.01)
            non_nullable_checks["vf64_mismatches"] = m

        results["non_nullable"] = non_nullable_checks
        results["non_nullable_valid"] = all(v == 0 for v in non_nullable_checks.values())

        # 3. Nullable columns: check presence and report validity
        #    These are EXPECTED TO FAIL due to inline null byte issue.
        nullable_checks = {}

        ni64 = columns.get("SENSOR$NI64", [])
        if ni64:
            m = sum(1 for a, b in zip(ids, ni64) if b is not None and int(a) != int(b))
            nullable_checks["ni64_mismatches"] = m

        ni32 = columns.get("SENSOR$NI32", [])
        if ni32:
            m = sum(1 for a, b in zip(ids, ni32) if b is not None and int(a) % (2**31) != int(b) % (2**31))
            nullable_checks["ni32_mismatches"] = m

        nf32 = columns.get("SENSOR$NF32", [])
        if nf32:
            m = sum(1 for a, b in zip(ids, nf32) if b is not None and abs(float(a) - float(b)) > 1.0)
            nullable_checks["nf32_mismatches"] = m

        nf64 = columns.get("SENSOR$NF64", [])
        if nf64:
            m = sum(1 for a, b in zip(ids, nf64) if b is not None and abs(float(a) * 0.5 - float(b)) > 0.01)
            nullable_checks["nf64_mismatches"] = m

        results["nullable"] = nullable_checks
        results["nullable_valid"] = all(v == 0 for v in nullable_checks.values())
        results["nullable_columns_present"] = sum(1 for k in columns if k.startswith("SENSOR$N"))

        # Overall
        results["all_valid"] = (
            results["id_no_duplicates"]
            and results["id_filter_valid"]
            and results["non_nullable_valid"]
            and results["nullable_valid"]
        )

        return results

    def summary(self) -> dict:
        with self._lock:
            total_rows = sum(s["rows"] for s in self._streams)
            total_batches = sum(s["batches"] for s in self._streams)
            all_valid = all(s["validations"].get("all_valid", False) for s in self._streams) if self._streams else False
            return {
                "total_rows": total_rows,
                "total_batches": total_batches,
                "all_valid": all_valid,
                "streams": list(self._streams),
            }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0, help="Port to listen on (0 = auto)")
    parser.add_argument("--timeout", type=int, default=30, help="Shutdown after N seconds")
    args = parser.parse_args()

    location = f"grpc://0.0.0.0:{args.port}"
    server = RecordingServer(location)

    print(f"FLIGHT_PORT={server.port}", flush=True)

    shutdown_event = threading.Event()

    def shutdown_handler(*_):
        shutdown_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    serve_thread = threading.Thread(target=server.serve, daemon=True)
    serve_thread.start()

    shutdown_event.wait(timeout=args.timeout)
    server.shutdown()

    print(json.dumps(server.summary()), flush=True)


if __name__ == "__main__":
    main()
