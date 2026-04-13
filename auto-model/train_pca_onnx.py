#!/usr/bin/env python3
"""Train an unsupervised reconstruction model on AutoVI data and export ONNX.

The exported model expects an input tensor named ``input`` with shape
``[batch, 3, image_size, image_size]`` and float32 values scaled to ``[0, 1]``.
It returns only:
  - ``anomaly_score``: mean squared reconstruction error per sample

Example:
    python3 train_pca_onnx.py \
        --dataset-root pipe_staple \
        --output pipe_staple_pca.onnx
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import numpy as np
import onnx
from onnx import TensorProto, checker, helper, numpy_helper
from PIL import Image


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a PCA reconstruction model on train/good and export it as ONNX."
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=Path("pipe_staple"),
        help="Dataset root containing train/good and test folders.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("pipe_staple_pca.onnx"),
        help="Path for the exported raw ONNX model.",
    )
    parser.add_argument(
        "--image-size",
        type=int,
        default=64,
        help="Square resize applied before training/export. Lower is faster.",
    )
    parser.add_argument(
        "--components",
        type=int,
        default=32,
        help="Number of PCA components to keep.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Batch size used only while loading images into memory.",
    )
    return parser.parse_args()


def iter_image_paths(root: Path) -> list[Path]:
    train_dir = root / "train" / "good"
    if not train_dir.is_dir():
        raise FileNotFoundError(f"Expected training directory at {train_dir}")
    paths = sorted(p for p in train_dir.iterdir() if p.suffix.lower() in {".png", ".jpg", ".jpeg"})
    if not paths:
        raise FileNotFoundError(f"No training images found in {train_dir}")
    return paths


def load_image(path: Path, image_size: int) -> np.ndarray:
    with Image.open(path) as image:
        image = image.convert("RGB")
        image = image.resize((image_size, image_size), Image.BILINEAR)
        array = np.asarray(image, dtype=np.float32) / 255.0
    return np.transpose(array, (2, 0, 1))


def batched(items: list[Path], batch_size: int) -> Iterable[list[Path]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def load_dataset(paths: list[Path], image_size: int, batch_size: int) -> np.ndarray:
    batches: list[np.ndarray] = []
    for batch_paths in batched(paths, batch_size):
        batch = np.stack([load_image(path, image_size) for path in batch_paths], axis=0)
        batches.append(batch)
    return np.concatenate(batches, axis=0)


def fit_pca(samples: np.ndarray, requested_components: int) -> tuple[np.ndarray, np.ndarray]:
    flat = samples.reshape(samples.shape[0], -1).astype(np.float64, copy=False)
    mean = flat.mean(axis=0, dtype=np.float64)
    centered = flat - mean

    max_rank = min(centered.shape[0], centered.shape[1])
    components_count = max(1, min(requested_components, max_rank))
    _, _, vt = np.linalg.svd(centered, full_matrices=False)
    components = vt[:components_count].astype(np.float32, copy=False)
    return mean.astype(np.float32, copy=False), components


def build_onnx_model(
    mean: np.ndarray,
    components: np.ndarray,
    image_size: int,
    dataset_root: Path,
) -> onnx.ModelProto:
    channels = 3
    flat_dim = channels * image_size * image_size
    component_count = int(components.shape[0])

    if mean.shape != (flat_dim,):
        raise ValueError(f"Expected mean shape {(flat_dim,)}, got {mean.shape}")
    if components.shape != (component_count, flat_dim):
        raise ValueError(
            f"Expected component matrix shape {(component_count, flat_dim)}, got {components.shape}"
        )

    initializers = [
        numpy_helper.from_array(mean, name="mean"),
        numpy_helper.from_array(components.T.astype(np.float32, copy=False), name="components_t"),
        numpy_helper.from_array(components.astype(np.float32, copy=False), name="components"),
    ]

    nodes = [
        helper.make_node("Flatten", ["input"], ["flat_input"], axis=1),
        helper.make_node("Sub", ["flat_input", "mean"], ["centered"]),
        helper.make_node("MatMul", ["centered", "components_t"], ["latent"]),
        helper.make_node("MatMul", ["latent", "components"], ["recon_centered"]),
        helper.make_node("Add", ["recon_centered", "mean"], ["recon_flat"]),
        helper.make_node("Sub", ["flat_input", "recon_flat"], ["residual"]),
        helper.make_node("Mul", ["residual", "residual"], ["squared_residual"]),
        helper.make_node(
            "ReduceMean",
            ["squared_residual"],
            ["anomaly_score"],
            axes=[1],
            keepdims=0,
        ),
    ]

    graph = helper.make_graph(
        nodes=nodes,
        name="pca_anomaly_detector",
        inputs=[
            helper.make_tensor_value_info(
                "input",
                TensorProto.FLOAT,
                ["batch", channels, image_size, image_size],
            )
        ],
        outputs=[
            helper.make_tensor_value_info("anomaly_score", TensorProto.FLOAT, ["batch"]),
        ],
        initializer=initializers,
    )

    model = helper.make_model(
        graph,
        producer_name="train_pca_onnx.py",
        producer_version="1.0",
        opset_imports=[helper.make_operatorsetid("", 13)],
    )
    model.ir_version = onnx.IR_VERSION

    metadata = {
        "model_type": "pca_reconstruction_anomaly_detector",
        "dataset_root": str(dataset_root),
        "image_size": str(image_size),
        "channels": str(channels),
        "num_components": str(component_count),
        "input_range": "0..1",
    }
    for key, value in metadata.items():
        entry = model.metadata_props.add()
        entry.key = key
        entry.value = value

    checker.check_model(model)
    return model


def main() -> None:
    args = parse_args()
    image_paths = iter_image_paths(args.dataset_root)
    samples = load_dataset(image_paths, args.image_size, args.batch_size)
    mean, components = fit_pca(samples, args.components)

    flat = samples.reshape(samples.shape[0], -1).astype(np.float32, copy=False)
    latent = (flat - mean) @ components.T
    recon = latent @ components + mean
    train_scores = np.mean((flat - recon) ** 2, axis=1)

    model = build_onnx_model(
        mean=mean,
        components=components,
        image_size=args.image_size,
        dataset_root=args.dataset_root,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    onnx.save(model, args.output)

    summary = {
        "dataset_root": str(args.dataset_root),
        "train_images": len(image_paths),
        "image_size": args.image_size,
        "components": int(components.shape[0]),
        "output": str(args.output),
        "train_score_mean": float(train_scores.mean()),
        "train_score_std": float(train_scores.std()),
        "train_score_p99_threshold": float(np.quantile(train_scores, 0.99)),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
