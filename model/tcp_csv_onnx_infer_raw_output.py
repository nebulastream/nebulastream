#!/usr/bin/env python3
"""Connect to a TCP server, read newline-delimited CSV rows, decode a base64 PNG,
convert it to the ONNX model input tensor, run inference, and print only raw model outputs.

Expected CSV schema per row:
    id,timestamp,width,height,data
where `data` is a base64-encoded PNG payload.
"""

from __future__ import annotations

import argparse
import base64
import csv
import io
import socket
import sys
from pathlib import Path
from typing import Any

import numpy as np
import onnxruntime as ort
from PIL import Image

CSV_FIELD_COUNT = 5


def raise_csv_field_limit() -> None:
    """Raise Python's CSV field size limit for very large base64 payloads."""
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10
            if limit <= 0:
                raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read CSV rows from TCP, decode base64 image payloads, run ONNX inference, and print only raw model outputs."
    )
    parser.add_argument("--host", required=True, help="TCP server host")
    parser.add_argument("--port", required=True, type=int, help="TCP server port")
    parser.add_argument("--model", required=True, help="Path to ONNX model")
    parser.add_argument(
        "--providers",
        nargs="+",
        default=["CPUExecutionProvider"],
        help="ONNX Runtime providers in priority order, e.g. CUDAExecutionProvider CPUExecutionProvider",
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="Socket connect/read timeout in seconds")
    parser.add_argument("--max-rows", type=int, default=0, help="Stop after this many rows. 0 means run forever.")
    parser.add_argument("--encoding", default="utf-8", help="Text encoding for CSV rows from the socket")
    parser.add_argument(
        "--assume-no-header",
        action="store_true",
        help="Treat the first row as data even if it looks like a header.",
    )
    parser.add_argument("--input-name", default=None, help="Optional ONNX input name override.")
    parser.add_argument("--resize-width", type=int, default=None, help="Optional width override.")
    parser.add_argument("--resize-height", type=int, default=None, help="Optional height override.")
    parser.add_argument(
        "--force-layout",
        choices=["nchw", "nhwc"],
        default=None,
        help="Optional layout override. By default inferred from the ONNX input shape.",
    )
    parser.add_argument(
        "--no-scale-01",
        action="store_true",
        help="Do not divide uint8 pixel values by 255. By default, values are scaled to [0,1].",
    )
    parser.add_argument(
        "--center-crop",
        action="store_true",
        help="Apply a center crop after resizing. Only use this if the model really expects it.",
    )
    parser.add_argument("--crop-width", type=int, default=None, help="Crop width when --center-crop is enabled.")
    parser.add_argument("--crop-height", type=int, default=None, help="Crop height when --center-crop is enabled.")
    parser.add_argument(
        "--normalize-imagenet",
        action="store_true",
        help="Apply ImageNet mean/std normalization after scaling to [0,1].",
    )
    parser.add_argument(
        "--print-as-json",
        action="store_true",
        help="Print raw outputs as JSON-compatible nested lists instead of NumPy repr.",
    )
    parser.add_argument("--save-last-input", default=None, help="Optional .npy path to save the last input tensor.")
    return parser.parse_args()


def choose_input_meta(session: ort.InferenceSession, explicit_name: str | None) -> ort.NodeArg:
    inputs = session.get_inputs()
    if not inputs:
        raise RuntimeError("The ONNX model has no inputs.")
    if explicit_name is None:
        return inputs[0]
    for item in inputs:
        if item.name == explicit_name:
            return item
    available = ", ".join(x.name for x in inputs)
    raise ValueError(f"Input '{explicit_name}' not found. Available inputs: {available}")


def infer_layout(shape: list[Any], forced: str | None) -> str:
    if forced:
        return forced
    if len(shape) != 4:
        raise ValueError(f"Expected 4D image input tensor, got shape {shape}.")
    if shape[1] in (1, 3):
        return "nchw"
    if shape[3] in (1, 3):
        return "nhwc"
    raise ValueError(f"Could not infer layout from ONNX input shape {shape}. Pass --force-layout.")


def resolved_hw(shape: list[Any], layout: str, width_override: int | None, height_override: int | None) -> tuple[int, int]:
    if layout == "nchw":
        model_h = shape[2]
        model_w = shape[3]
    else:
        model_h = shape[1]
        model_w = shape[2]
    height = height_override if height_override is not None else model_h
    width = width_override if width_override is not None else model_w
    if not isinstance(height, int) or not isinstance(width, int):
        raise ValueError("The ONNX model uses dynamic or unknown spatial dimensions. Pass --resize-height/--resize-width.")
    return width, height


def center_crop(img: Image.Image, crop_w: int, crop_h: int) -> Image.Image:
    w, h = img.size
    if crop_w > w or crop_h > h:
        raise ValueError(f"Crop size {(crop_w, crop_h)} is larger than image size {(w, h)}")
    left = (w - crop_w) // 2
    top = (h - crop_h) // 2
    return img.crop((left, top, left + crop_w, top + crop_h))


def decode_png_base64_to_model_input(
    b64_png: str,
    *,
    width: int,
    height: int,
    layout: str,
    scale_01: bool,
    do_center_crop: bool,
    crop_width: int | None,
    crop_height: int | None,
    normalize_imagenet: bool,
) -> np.ndarray:
    png_bytes = base64.b64decode(b64_png)
    image = Image.open(io.BytesIO(png_bytes)).convert("RGB")
    image = image.resize((width, height), Image.BILINEAR)

    if do_center_crop:
        crop_w = crop_width or width
        crop_h = crop_height or height
        image = center_crop(image, crop_w, crop_h)

    arr = np.asarray(image, dtype=np.float32)
    if scale_01:
        arr /= 255.0

    if layout == "nchw":
        arr = np.transpose(arr, (2, 0, 1))
    elif layout != "nhwc":
        raise ValueError(f"Unsupported layout: {layout}")

    if normalize_imagenet:
        mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
        std = np.array([0.229, 0.224, 0.225], dtype=np.float32)
        if layout == "nchw":
            mean = mean.reshape(3, 1, 1)
            std = std.reshape(3, 1, 1)
        else:
            mean = mean.reshape(1, 1, 3)
            std = std.reshape(1, 1, 3)
        arr = (arr - mean) / std

    arr = np.expand_dims(arr, axis=0)
    return arr.astype(np.float32, copy=False)


def validate_input_tensor_shape(tensor: np.ndarray, onnx_shape: list[Any]) -> None:
    if len(tensor.shape) != len(onnx_shape):
        raise ValueError(f"Prepared input rank {tensor.shape} does not match model input shape {onnx_shape}")
    for actual, expected in zip(tensor.shape, onnx_shape):
        if isinstance(expected, int) and expected > 0 and actual != expected:
            raise ValueError(
                f"Prepared input shape {list(tensor.shape)} does not match model input shape {onnx_shape}. "
                f"Check resize/crop flags."
            )


def looks_like_header(row: list[str]) -> bool:
    header = [x.strip().lower() for x in row]
    return header[:5] == ["id", "timestamp", "width", "height", "data"]


def main() -> int:
    args = parse_args()
    raise_csv_field_limit()

    model_path = Path(args.model).expanduser().resolve()
    if not model_path.exists():
        raise FileNotFoundError(f"ONNX model not found: {model_path}")

    session = ort.InferenceSession(str(model_path), providers=args.providers)
    input_meta = choose_input_meta(session, args.input_name)
    input_name = input_meta.name
    input_shape = list(input_meta.shape)
    layout = infer_layout(input_shape, args.force_layout)
    resize_w, resize_h = resolved_hw(input_shape, layout, args.resize_width, args.resize_height)
    output_names = [x.name for x in session.get_outputs()]

    processed = 0
    with socket.create_connection((args.host, args.port), timeout=args.timeout) as sock:
        sock.settimeout(args.timeout)
        with sock.makefile("r", encoding=args.encoding, newline="") as fileobj:
            reader = csv.reader(fileobj)
            for row in reader:
                if not row:
                    continue
                if len(row) < CSV_FIELD_COUNT:
                    print(f"Skipping malformed row with {len(row)} fields: {row[:3]}", file=sys.stderr)
                    continue
                if processed == 0 and not args.assume_no_header and looks_like_header(row):
                    continue

                row_id, timestamp_ms, src_w, src_h, data_b64 = row[:5]
                try:
                    tensor = decode_png_base64_to_model_input(
                        data_b64,
                        width=resize_w,
                        height=resize_h,
                        layout=layout,
                        scale_01=not args.no_scale_01,
                        do_center_crop=args.center_crop,
                        crop_width=args.crop_width,
                        crop_height=args.crop_height,
                        normalize_imagenet=args.normalize_imagenet,
                    )
                    validate_input_tensor_shape(tensor, input_shape)
                    outputs = session.run(output_names, {input_name: tensor})
                except Exception as exc:
                    print(f"Inference failed for row id={row_id}, timestamp={timestamp_ms}: {exc}", file=sys.stderr)
                    continue

                if args.save_last_input:
                    np.save(args.save_last_input, tensor)

                if args.print_as_json:
                    print([np.asarray(output).tolist() for output in outputs])
                else:
                    print(outputs)
                sys.stdout.flush()

                processed += 1
                if args.max_rows and processed >= args.max_rows:
                    break

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
