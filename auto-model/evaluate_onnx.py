#!/usr/bin/env python3
"""Evaluate an ONNX anomaly model on an AutoVI dataset test split."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import onnxruntime as ort
from PIL import Image


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate a single-output ONNX anomaly model on an AutoVI test split."
    )
    parser.add_argument(
        "--model",
        type=Path,
        required=True,
        help="Path to the ONNX model.",
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        required=True,
        help="Dataset root containing the test folder.",
    )
    parser.add_argument(
        "--good-label",
        type=str,
        default="good",
        help="Name of the normal test subdirectory.",
    )
    return parser.parse_args()


def load_session(model_path: Path) -> tuple[ort.InferenceSession, str, int, int]:
    session = ort.InferenceSession(str(model_path), providers=["CPUExecutionProvider"])
    input_meta = session.get_inputs()[0]
    height = int(input_meta.shape[2])
    width = int(input_meta.shape[3])
    return session, input_meta.name, height, width


def load_image(path: Path, height: int, width: int) -> np.ndarray:
    with Image.open(path) as image:
        image = image.convert("RGB")
        image = image.resize((width, height), Image.BILINEAR)
        array = np.asarray(image, dtype=np.float32) / 255.0
    return np.transpose(array, (2, 0, 1))


def score_paths(
    session: ort.InferenceSession, input_name: str, height: int, width: int, paths: list[Path]
) -> np.ndarray:
    batch = np.stack([load_image(path, height, width) for path in paths]).astype(np.float32)
    return session.run(None, {input_name: batch})[0]


def summarize_scores(scores: np.ndarray) -> dict[str, float | int]:
    return {
        "n": int(scores.shape[0]),
        "min": float(scores.min()),
        "median": float(np.median(scores)),
        "max": float(scores.max()),
        "mean": float(scores.mean()),
    }


def best_threshold(scores: np.ndarray, labels: np.ndarray) -> dict[str, float | int]:
    best: tuple[float, float, float, float, int, int, int, int] | None = None
    for threshold in np.unique(scores):
        pred = (scores > threshold).astype(int)
        tp = int(((pred == 1) & (labels == 1)).sum())
        tn = int(((pred == 0) & (labels == 0)).sum())
        fp = int(((pred == 1) & (labels == 0)).sum())
        fn = int(((pred == 0) & (labels == 1)).sum())
        tpr = tp / (tp + fn)
        tnr = tn / (tn + fp)
        balanced_accuracy = (tpr + tnr) / 2
        accuracy = (tp + tn) / len(labels)
        f1 = (2 * tp) / (2 * tp + fp + fn) if (2 * tp + fp + fn) else 0.0
        candidate = (balanced_accuracy, accuracy, f1, float(threshold), tp, tn, fp, fn)
        if best is None or candidate > best:
            best = candidate

    assert best is not None
    balanced_accuracy, accuracy, f1, threshold, tp, tn, fp, fn = best
    return {
        "threshold": threshold,
        "balanced_accuracy": balanced_accuracy,
        "accuracy": accuracy,
        "f1": f1,
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
    }


def main() -> None:
    args = parse_args()
    test_root = args.dataset_root / "test"
    if not test_root.is_dir():
        raise FileNotFoundError(f"Expected test directory at {test_root}")

    session, input_name, height, width = load_session(args.model)
    class_scores: dict[str, np.ndarray] = {}

    for defect_dir in sorted(path for path in test_root.iterdir() if path.is_dir()):
        paths = sorted(defect_dir.glob("*.png"))
        if not paths:
            continue
        class_scores[defect_dir.name] = score_paths(session, input_name, height, width, paths)

    if args.good_label not in class_scores:
        raise ValueError(f"Expected a '{args.good_label}' directory under {test_root}")

    good_scores = class_scores[args.good_label]
    anomaly_scores = np.concatenate(
        [scores for name, scores in class_scores.items() if name != args.good_label], axis=0
    )
    all_scores = np.concatenate([good_scores, anomaly_scores], axis=0)
    labels = np.array([0] * len(good_scores) + [1] * len(anomaly_scores), dtype=np.int64)

    report = {
        "model": str(args.model),
        "dataset_root": str(args.dataset_root),
        "classes": {name: summarize_scores(scores) for name, scores in class_scores.items()},
        "reference_threshold": best_threshold(all_scores, labels),
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
