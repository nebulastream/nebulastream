MODEL INPUT

The ONNX/IREE model expects a numeric tensor, not a string. For your current export, that tensor should be:

- float32
- shape [1, 3, 256, 256]
- NCHW layout


MODEL OUTPUT

The actual ONNX model output is just the two tensors:
1. output named "output"
- raw tensor shape: [1]
- raw value: approximately 32.9896125793457

2. output named "648"
- raw tensor shape: [1, 1, 256, 256]
- raw tensor values: the full 256×256 float map


to run the script:

python tcp_csv_onnx_infer_raw_output.py \
  --host 127.0.0.1 \
  --port 8080 \
  --model ./exports/patchcore_transistor_onnx/patchcore_transistor_static.onnx \
  --normalize-imagenet
  
If you want the raw outputs as plain JSON-compatible nested lists instead of NumPy array(...) repr, use:

python tcp_csv_onnx_infer_raw_output.py \
  --host 127.0.0.1 \
  --port 8080 \
  --model ./exports/patchcore_transistor_onnx/patchcore_transistor_static.onnx \
  --normalize-imagenet \
  --print-as-json

