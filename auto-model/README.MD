# AutoVI ONNX Training

This workspace contains a training script that learns an unsupervised PCA reconstruction model from the AutoVI `pipe_staple` dataset and exports the trained model in raw ONNX format.

The exported ONNX model returns only one value per sample: a normalized `anomaly_score` in `[0, 1]`.

## Files

- `train_pca_onnx.py`: trains the model and exports ONNX
- `infer_onnx.py`: runs inference on one or more images with the exported ONNX model
- `evaluate_onnx.py`: evaluates an ONNX model on a full AutoVI test split and reports a reference threshold
- `pipe_staple/`: dataset root
- `engine_wiring/`: dataset root
- `pipe_clip/`: dataset root
- `pipe_staple_pca.onnx`: example exported model generated from the local dataset
- `engine_wiring_pca.onnx`: exported ONNX model for `engine_wiring`
- `pipe_clip_pca.onnx`: exported ONNX model for `pipe_clip`

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

Additional datasets:

```bash
python3 train_pca_onnx.py --dataset-root engine_wiring --output engine_wiring_pca.onnx
python3 train_pca_onnx.py --dataset-root pipe_clip --output pipe_clip_pca.onnx
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
- `--score-lower-quantile`: lower train-score quantile used for normalization, default `0.01`
- `--score-upper-quantile`: upper train-score quantile used for normalization, default `0.99`

## Output Model

The exported ONNX model:

- Input name: `input`
- Input shape: `[batch, 3, 64, 64]` by default
- Input dtype: `float32`
- Input range: `[0, 1]`
- Output name: `anomaly_score`
- Output shape: `[batch]`
- Output dtype: `float32`
- Output range: `[0, 1]`

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
range: [0.0, 1.0]
meaning: normalized anomaly score per image
```

Interpretation:

- the ONNX model does not perform thresholding, labeling, or any other decision logic
- threshold selection is external to the model
- higher scores indicate larger reconstruction error
- the score is normalized inside the ONNX graph with a sigmoid mapping:

```text
anomaly_score = sigmoid((raw_score - score_center) / score_temperature)
```

## Example Training Result

The local verified run produced:

- `191` training images
- image size `64`
- `32` PCA components
- normalization lower quantile `p01 = 0.0022581803612411024`
- normalization upper quantile `p99 = 0.008084132708609103`
- score center `0.005171156534925102`
- score temperature `0.001456488086842`

Additional verified runs:

- `engine_wiring`: `285` training images, `32` PCA components, normalization `p01 = 0.005535541493445635`, `p99 = 0.013168731182813646`, center `0.00935213633812964`, temperature `0.0019082974223420027`
- `pipe_clip`: `195` training images, `32` PCA components, normalization `p01 = 0.0044828812964260575`, `p99 = 0.01309773623943329`, center `0.008790308767929673`, temperature `0.0021537137357518082`

## Normalization Notes

The ONNX output is normalized using train-good score quantiles, but with a smooth sigmoid instead of hard clipping:

```text
score_center = (p01 + p99) / 2
score_temperature = (p99 - p01) / 4
normalized_score = sigmoid((raw_score - score_center) / score_temperature)
```

This keeps the model output in `[0, 1]` while preserving more score spread above the train-good range.

For `pipe_staple`, the normalized labeled test-score ranges are:

- `good`: min `0.14444081485271454`, median `0.5823087692260742`, max `1.0`
- `missing`: min `0.8996009826660156`, median `0.9806038737297058`, max `1.0`

A practical reference threshold on the normalized scale for `pipe_staple` is:

```text
0.9088001251220703
```

At threshold `0.9088001251220703` on the labeled `pipe_staple` test split:

- `116 / 117` defective images are detected
- `128 / 188` good images are correctly classified
- `60` good images are false positives
- `1` defective image is a false negative
- overall accuracy is `0.80`
- balanced accuracy is `0.836`

This threshold is still tuned on the labeled test split, so it should be treated as a reference value rather than a production calibration.

For `engine_wiring`, the normalized labeled test-score ranges are:

- `good`: min `0.1405777633190155`, median `0.6212978959083557`, max `0.9999912977218628`
- `blue_hoop`: min `0.7276800870895386`, median `0.8911786675453186`, max `0.9455253481864929`
- `cardboard`: min `1.0`, median `1.0`, max `1.0`
- `fastening`: min `0.2175666093826294`, median `0.8358016014099121`, max `0.9999990463256836`
- `multiple`: min `0.6236893534660339`, median `0.9602231979370117`, max `1.0`
- `obstruction`: min `0.9999110698699951`, median `0.9999550580978394`, max `0.9999990463256836`

A practical reference threshold on the normalized scale for `engine_wiring` is:

```text
0.7700981497764587
```

At threshold `0.7700981497764587` on the labeled `engine_wiring` test split:

- `219 / 322` defective images are detected
- `198 / 285` good images are correctly classified
- `87` good images are false positives
- `103` defective images are false negatives
- overall accuracy is `0.6869851729818781`
- balanced accuracy is `0.6874305328538739`

For `pipe_clip`, the normalized labeled test-score ranges are:

- `good`: min `0.12218306213617325`, median `0.7655408382415771`, max `1.0`
- `operator`: min `0.9999998807907104`, median `0.9999998807907104`, max `0.9999998807907104`
- `unclipped`: min `0.4149962365627289`, median `0.9495592713356018`, max `1.0`

A practical reference threshold on the normalized scale for `pipe_clip` is:

```text
0.6284769177436829
```

At threshold `0.6284769177436829` on the labeled `pipe_clip` test split:

- `137 / 142` defective images are detected
- `77 / 195` good images are correctly classified
- `118` good images are false positives
- `5` defective images are false negatives
- overall accuracy is `0.6350148367952523`
- balanced accuracy is `0.6798302636330805`

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

Other datasets:

```bash
python3 infer_onnx.py --model engine_wiring_pca.onnx engine_wiring/test/good/0000.png
python3 infer_onnx.py --model pipe_clip_pca.onnx pipe_clip/test/good/0000.png
```

Example output fields:

- `image`
- `anomaly_score`

Example JSON output:

```json
[
  {
    "image": "pipe_staple/test/missing/0000.png",
    "anomaly_score": 1.0
  }
]
```

## Evaluate A Model

Evaluate a full test split and compute a reference threshold:

```bash
python3 evaluate_onnx.py --model pipe_staple_pca.onnx --dataset-root pipe_staple
python3 evaluate_onnx.py --model engine_wiring_pca.onnx --dataset-root engine_wiring
python3 evaluate_onnx.py --model pipe_clip_pca.onnx --dataset-root pipe_clip
```

The evaluation output includes:

- per-class score ranges and means
- a reference threshold on the normalized `[0,1]` score scale
- balanced accuracy
- accuracy
- F1 score
- `tp`, `tn`, `fp`, `fn`

## Notes

- The script converts source images from RGBA to RGB before training.
- The inference script converts source images from RGBA to RGB before scoring.
- The model is a lightweight PCA-based baseline, chosen to avoid requiring PyTorch in this environment.
- If you change `--image-size`, inference inputs must use the same size as the trained model.
- Pruning is not used here; exposing one float from ONNX is an interface/export decision rather than a pruning requirement.
