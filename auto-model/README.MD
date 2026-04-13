# AutoVI ONNX Training

This workspace contains a training script that learns an unsupervised PCA reconstruction model from the AutoVI `pipe_staple` dataset and exports the trained model in raw ONNX format.

The exported ONNX model returns only one value per sample: `anomaly_score`.

## Files

- `train_pca_onnx.py`: trains the model and exports ONNX
- `infer_onnx.py`: runs inference on one or more images with the exported ONNX model
- `pipe_staple/`: dataset root
- `pipe_staple_pca.onnx`: example exported model generated from the local dataset

## Requirements

The script uses these Python packages:

- `numpy`
- `Pillow`
- `onnx`

Optional for validation:

- `onnxruntime`

## Dataset Layout

The script expects this structure inside the dataset root:

```text
pipe_staple/
  train/
    good/
  test/
    good/
    missing/
  ground_truth/
```

Training uses only `train/good`, which matches the unsupervised anomaly-detection setup of the dataset.

## Run Training

From this directory:

```bash
python3 train_pca_onnx.py --dataset-root pipe_staple --output pipe_staple_pca.onnx
```

## Options

```bash
python3 train_pca_onnx.py --help
```

Main options:

- `--dataset-root`: dataset directory, default `pipe_staple`
- `--output`: output ONNX path, default `pipe_staple_pca.onnx`
- `--image-size`: resize target for training and inference, default `64`
- `--components`: PCA component count, default `32`
- `--batch-size`: image loading batch size, default `32`

## Output Model

The exported ONNX model:

- Input name: `input`
- Input shape: `[batch, 3, 64, 64]` by default
- Input dtype: `float32`
- Input range: `[0, 1]`
- Output name: `anomaly_score`
- Output shape: `[batch]`
- Output dtype: `float32`

## ONNX Input And Output Format

### ONNX Input

The ONNX graph accepts one input tensor:

```text
name: input
shape: [batch, 3, 64, 64]
dtype: float32
layout: NCHW
range: [0.0, 1.0]
channels: RGB
```

Details:

- `batch` can be any positive number of images
- `3` is the number of color channels
- `64 x 64` is the default training and inference size
- if you retrain with another `--image-size`, the ONNX input shape changes to `[batch, 3, image_size, image_size]`

### ONNX Output

The ONNX graph returns one output:

```text
name: anomaly_score
shape: [batch]
dtype: float32
meaning: mean squared reconstruction error per image
```

Interpretation:

- the ONNX model does not perform thresholding, labeling, or any other decision logic
- threshold selection is external to the model
- higher scores indicate larger reconstruction error

## Example Training Result

The local verified run produced:

- `191` training images
- image size `64`
- `32` PCA components

## Suggested Threshold

For the current exported model, a practical reference threshold is:

```text
0.008519727736711502
```

This value was measured on the labeled dataset split:

- `pipe_staple/test/good`
- `pipe_staple/test/missing`

Observed score ranges for the current model:

- `good`: min `0.0025802310556173325`, median `0.005655089393258095`, max `0.030224964022636414`
- `missing`: min `0.008364940993487835`, median `0.010885094292461872`, max `0.06458847969770432`

At threshold `0.008519727736711502`:

- `116 / 117` defective images are detected
- `128 / 188` good images are correctly classified
- `60` good images are false positives
- `1` defective image is a false negative
- overall accuracy is `0.80`
- balanced accuracy is `0.836`

Important:

- this threshold was tuned on the labeled test split, so it is useful as a reference but should not be treated as a production-calibrated threshold
- there is overlap between good and defective score distributions, so this PCA baseline does not provide a clean separation
- for deployment, threshold tuning should be done on a separate validation set

## Verify The Exported Model

If `onnxruntime` is installed, you can check that the model loads and runs:

```bash
python3 -c "import numpy as np, onnxruntime as ort; s=ort.InferenceSession('pipe_staple_pca.onnx', providers=['CPUExecutionProvider']); x=np.random.rand(2,3,64,64).astype(np.float32); y=s.run(None, {'input': x}); print([a.shape for a in y], y[0].dtype)"
```

Expected shape pattern:

```text
[(2,)]
```

## Run Inference

Score one or more images:

```bash
python3 infer_onnx.py --model pipe_staple_pca.onnx pipe_staple/test/good/0000.png pipe_staple/test/missing/0000.png
```

Example output fields:

- `image`
- `anomaly_score`

Example JSON output:

```json
[
  {
    "image": "pipe_staple/test/missing/0000.png",
    "anomaly_score": 0.010732476599514484
  }
]
```

## Notes

- The script converts source images from RGBA to RGB before training.
- The inference script converts source images from RGBA to RGB before scoring.
- The model is a lightweight PCA-based baseline, chosen to avoid requiring PyTorch in this environment.
- If you change `--image-size`, inference inputs must use the same size as the trained model.
- Pruning is not used here; exposing one float from ONNX is an interface/export decision rather than a pruning requirement.
