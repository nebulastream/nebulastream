#!/usr/bin/env python3
"""End-to-end demo for the ThermalImage / to_celsius / is_fever / to_rgb PoC.

Pipeline:
  1. Synthesize two 64x64 mono16 thermal images of a stylized face. The
     "healthy" record has a cool background (~24 degC), warmer face (~32 degC),
     cool eye dots, and a normal-temperature forehead. The "fever" record is
     identical except that the forehead patch is at ~39 degC, exceeding the
     38 degC is_fever threshold. Both records share the same input file so
     the same query produces two output records that differ only in is_fever
     and on the forehead pixels of the rendered Celsius/RGB outputs.
  2. Encode both images as line-delimited JSON in the schema the registered
     ThermalImage type expects: {"timestamp": ..., "frame": {"pixels": [...]}}.
  3. Pipe SQL into nes-repl-embedded with --on-exit WAIT_FOR_QUERY_TERMINATION.
     The query selects timestamp, to_celsius(frame), is_fever(frame),
     to_rgb(frame, 'iron'). Output goes to a JSON sink file.
  4. Parse the JSON output (one record per line, after the schema header) and
     rasterize each result to PNG: input, grayscale Celsius, iron-palette RGB.

The whole thing runs from the host using the binaries built inside the docker
toolchain build directory; no separate worker process is needed.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import numpy as np
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parents[4]
BUILD_DIR = REPO_ROOT / "cmake-build-debug-docker-nes-development-local-libstd"
NES_REPL = BUILD_DIR / "nes-frontend" / "apps" / "nes-repl-embedded"

DEMO_DIR = Path(__file__).resolve().parent
INPUT_JSON = DEMO_DIR / "input.json"
OUTPUT_JSON = DEMO_DIR / "output.json"

WIDTH = 64
HEIGHT = 64
PIXEL_COUNT = WIDTH * HEIGHT  # must match THERMAL_IMAGE_PIXEL_COUNT in ImageDataType.cpp

# Mono16 pixels are encoded as centi-Kelvin: raw / 100 = Kelvin, K = C + 273.15.
def celsius_to_raw(c: float) -> int:
    return int(round((c + 273.15) * 100))

COOL_BACKGROUND   = celsius_to_raw(24.0)   # ~29715
FACE_WARM         = celsius_to_raw(31.5)   # ~30465
EYE_COOL          = celsius_to_raw(28.0)   # ~30115
FOREHEAD_NORMAL   = celsius_to_raw(33.0)   # ~30615 — well below the 31115 fever threshold
FOREHEAD_FEVER    = celsius_to_raw(39.0)   # ~31215 — above the threshold


def synthesize_thermal_image(*, fever: bool) -> np.ndarray:
    """Stylized thermal face. The forehead patch is the only difference between
    the healthy (`fever=False`) and fever (`fever=True`) variants — picked so
    the rest of the rendered image is byte-for-byte identical except for that
    patch and so is_fever flips between False and True."""
    img = np.full((HEIGHT, WIDTH), COOL_BACKGROUND, dtype=np.uint16)
    yy, xx = np.ogrid[:HEIGHT, :WIDTH]

    head_mask = (yy - 34) ** 2 + (xx - 32) ** 2 < 24 ** 2
    img[head_mask] = FACE_WARM

    left_eye  = (yy - 30) ** 2 + (xx - 24) ** 2 < 3 ** 2
    right_eye = (yy - 30) ** 2 + (xx - 40) ** 2 < 3 ** 2
    img[left_eye | right_eye] = EYE_COOL

    forehead = (yy - 19) ** 2 + (xx - 32) ** 2 < 5 ** 2
    img[forehead] = FOREHEAD_FEVER if fever else FOREHEAD_NORMAL

    # Mouth: a horizontal line of slightly warmer-than-face pixels for shape recognition.
    mouth_y = 44
    img[mouth_y:mouth_y + 2, 26:39] = celsius_to_raw(33.5)

    return img


def save_grayscale_png(img16: np.ndarray, path: Path) -> None:
    """Map UINT16 mono16 to UINT8 by taking the high byte (matches the
    'grayscale' colormap in ToRGBPhysicalFunction). Upscales 4x for visibility."""
    img8 = (img16 >> 8).astype(np.uint8)
    Image.fromarray(img8, mode="L").resize(
        (img8.shape[1] * 4, img8.shape[0] * 4), Image.NEAREST
    ).save(path)


def write_input_json(images: list[tuple[int, np.ndarray]]) -> None:
    """Encode each (timestamp, image) as one ThermalImage record per line."""
    with INPUT_JSON.open("w") as fh:
        for timestamp, img16 in images:
            record = {"timestamp": timestamp, "frame": {"pixels": img16.flatten().tolist()}}
            fh.write(json.dumps(record) + "\n")


def build_sql() -> str:
    """SQL statements piped into nes-repl-embedded.

    Uses the inline `INTO File(...)` sink with OUTPUT_FORMAT, FILE_PATH and HOST
    set via SET config — schema is auto-inferred from the SELECT projection.
    """
    return f"""\
CREATE LOGICAL SOURCE thermalRecords(`timestamp` UINT64 NOT NULL, `frame` ThermalImage NOT NULL);

CREATE PHYSICAL SOURCE FOR thermalRecords TYPE File
SET(
    'NestedJSON' AS `PARSER`.`TYPE`,
    '{INPUT_JSON}' AS `SOURCE`.FILE_PATH
);

SELECT
    `timestamp`,
    to_celsius(`frame`) AS celsius,
    is_fever(`frame`) AS fever,
    to_rgb(`frame`, VARSIZED('iron')) AS rgb
FROM thermalRecords
INTO File(
    'JSON' AS `SINK`.OUTPUT_FORMAT,
    '{OUTPUT_JSON}' AS `SINK`.FILE_PATH,
    'localhost:8080' AS `SINK`.HOST
);
"""


def run_query() -> None:
    if not NES_REPL.exists():
        sys.exit(
            f"nes-repl-embedded not found at {NES_REPL}; build it with:\n"
            f"  .claude/skills/nes-build/in-docker.sh cmake --build "
            f"/tmp/nebulastream-public/cmake-build-debug-docker-nes-development-local-libstd "
            f"--target nes-repl-embedded -j"
        )
    if OUTPUT_JSON.exists():
        OUTPUT_JSON.unlink()

    sql = build_sql()
    print("--- SQL ---")
    print(sql)
    print("--- repl output ---")
    proc = subprocess.run(
        [str(NES_REPL), "--on-exit", "WAIT_FOR_QUERY_TERMINATION", "-e", "FAIL_FAST", "-f", "JSON"],
        input=sql, text=True, capture_output=True, check=False,
    )
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        sys.exit(f"nes-repl-embedded exited {proc.returncode}")
    if not OUTPUT_JSON.exists():
        sys.exit(f"sink file not produced at {OUTPUT_JSON}")


def render_record(record: dict, label: str) -> None:
    """Rasterize one output record to <label>'s grayscale and iron PNGs.

    Field names follow the SCHEMA$column convention except for AS-aliased
    projections, which appear as the bare uppercased alias.
    """
    celsius = np.array(record["CELSIUS"], dtype=np.float32).reshape(HEIGHT, WIDTH)
    fever = bool(record["FEVER"])
    rgb_struct = record["RGB"]
    r = np.array(rgb_struct["r"], dtype=np.uint8).reshape(HEIGHT, WIDTH)
    g = np.array(rgb_struct["g"], dtype=np.uint8).reshape(HEIGHT, WIDTH)
    b = np.array(rgb_struct["b"], dtype=np.uint8).reshape(HEIGHT, WIDTH)
    rgb = np.stack([r, g, b], axis=-1)

    cmin, cmax = float(celsius.min()), float(celsius.max())
    print(
        f"  {label:>8s}: is_fever={fever}, "
        f"celsius range {cmin:.2f}..{cmax:.2f} degC"
    )

    # Grayscale rendering of the Celsius output: linearly map [min, max] -> 0..255.
    if cmax > cmin:
        gray = ((celsius - cmin) / (cmax - cmin) * 255.0).astype(np.uint8)
    else:
        gray = np.zeros_like(celsius, dtype=np.uint8)
    Image.fromarray(gray, mode="L").resize(
        (WIDTH * 4, HEIGHT * 4), Image.NEAREST
    ).save(DEMO_DIR / f"output_celsius_{label}.png")

    # RGB rendering of the iron output. Same 4x upscale for visibility.
    Image.fromarray(rgb, mode="RGB").resize(
        (WIDTH * 4, HEIGHT * 4), Image.NEAREST
    ).save(DEMO_DIR / f"output_iron_{label}.png")


def render_outputs(labels_by_timestamp: dict[int, str]) -> None:
    """File JSON sink writes a `name:type:nullable,...` header on the first
    line and one JSON object per record on subsequent lines.

    Per-tuple order is not guaranteed by the streaming engine (different
    records can land on different pipelines and arrive in any order), so we
    parse all records first and look up the label by the timestamp the
    synthesizer assigned. That keeps the demo deterministic regardless of
    which record finishes first.
    """
    lines = OUTPUT_JSON.read_text().strip().splitlines()
    if len(lines) < 1 + len(labels_by_timestamp):
        sys.exit(
            f"output JSON has {len(lines)} lines, expected 1 (header) + "
            f"{len(labels_by_timestamp)} (records)"
        )
    records = [json.loads(line) for line in lines[1:]]
    print("Per-record results:")
    for record in records:
        timestamp = int(record["THERMALRECORDS$timestamp"])
        label = labels_by_timestamp.get(timestamp)
        if label is None:
            sys.exit(f"unexpected timestamp {timestamp} in output")
        render_record(record, label)


def main() -> None:
    healthy = synthesize_thermal_image(fever=False)
    fever = synthesize_thermal_image(fever=True)
    save_grayscale_png(healthy, DEMO_DIR / "input_healthy.png")
    save_grayscale_png(fever, DEMO_DIR / "input_fever.png")
    write_input_json([(1, healthy), (2, fever)])

    run_query()
    render_outputs(labels_by_timestamp={1: "healthy", 2: "fever"})

    print(
        "\nWrote: input_{healthy,fever}.png, "
        "output_celsius_{healthy,fever}.png, output_iron_{healthy,fever}.png"
    )


if __name__ == "__main__":
    main()
