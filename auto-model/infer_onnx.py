#!/usr/bin/env python3
"""Run inference with the exported PCA ONNX anomaly detector."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import onnxruntime as ort
from PIL import Image


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run anomaly inference with an exported ONNX model."
    )
    parser.add_argument(
        "--model",
        type=Path,
        default=Path("pipe_staple_pca.onnx"),
        help="Path to the exported ONNX model.",
    )
    parser.add_argument(
        "images",
        nargs="+",
        type=Path,
        help="One or more image paths to score.",
    )
    return parser.parse_args()


def get_input_hw(session: ort.InferenceSession) -> tuple[int, int]:
    input_shape = session.get_inputs()[0].shape
    if len(input_shape) != 4:
        raise ValueError(f"Expected 4D input tensor, got {input_shape}")
    height = int(input_shape[2])
    width = int(input_shape[3])
    return height, width


def load_image(path: Path, height: int, width: int) -> np.ndarray:
    with Image.open(path) as image:
        image = image.convert("RGB")
        image = image.resize((width, height), Image.BILINEAR)
        array = np.asarray(image, dtype=np.float32) / 255.0
    return np.transpose(array, (2, 0, 1))


def main() -> None:
    args = parse_args()
    session = ort.InferenceSession(str(args.model), providers=["CPUExecutionProvider"])
    input_name = session.get_inputs()[0].name
    height, width = get_input_hw(session)

    batch = np.stack([load_image(path, height, width) for path in args.images], axis=0).astype(
        np.float32
    )
    scores = session.run(None, {input_name: batch})[0]

    results = []
    for index, image_path in enumerate(args.images):
        score = float(scores[index])
        result = {
            "image": str(image_path),
            "anomaly_score": score,
        }
        results.append(result)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
