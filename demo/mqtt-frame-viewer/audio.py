#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10,<3.12"
# dependencies = [
#     "nemo_toolkit[asr]>=2.0",
#     "onnx",
#     "onnxruntime",
#     "numpy",
# ]
# ///
"""Export MatchboxNet KWS to a features-in ONNX graph + everything the C++
frontend needs to reproduce NeMo's MFCC.

    uv run audio.py [outdir]

Writes to ./kws_export:
    kws.onnx          [1, n_frames, n_mfcc] float32 -> [1, n_classes] float32
    manifest.json     labels, MFCC config, shapes/dtypes of every .bin
    buf_*.bin         preprocessor buffers (mel filterbank, DCT matrix, window)
    ref_*_audio.bin   reference 16000-sample windows
    ref_*_feat.bin    NeMo's MFCC output for each  <- match this element-wise
    ref_*_probabilities.bin  normalized model output for each

All .bin are flat little-endian float32, C order.
"""

import json
import sys
from pathlib import Path

import numpy as np
import onnxruntime as ort
import torch
import torch.nn as nn

MODEL = "commandrecognition_en_matchboxnet3x1x64_v2_subset_task"
SR, WINDOW = 16000, 16000


class KWS(nn.Module):
    """Features in, probabilities out; the OpenCV MFCC preprocessor stays outside."""

    def __init__(self, m):
        super().__init__()
        self.enc, self.dec = m.encoder, m.decoder

    def forward(self, feats):  # [1, n_frames, n_mfcc]
        feats = feats.transpose(1, 2)
        n = torch.tensor([feats.shape[-1]], dtype=torch.int64)
        logits = self.dec(encoder_output=self.enc(audio_signal=feats, length=n)[0])
        return torch.softmax(logits, dim=-1)


def ref_signals():
    """Noise exercises every filterbank bin; the tone catches mel-scale and bin
    alignment errors; silence catches log-floor and DC handling."""
    t = torch.arange(WINDOW) / SR
    g = torch.Generator().manual_seed(0)
    return {
        "noise": torch.randn(WINDOW, generator=g) * 0.1,
        "tone1k": torch.sin(2 * torch.pi * 1000 * t) * 0.5,
        "chirp": torch.sin(2 * torch.pi * (200 + 3000 * t) * t) * 0.5,
        "silence": torch.zeros(WINDOW),
    }


def main():
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "kws_export")
    out.mkdir(parents=True, exist_ok=True)
    man = {"model": MODEL, "sample_rate": SR, "window_samples": WINDOW, "bins": {}}

    def dump(name, t):
        a = np.ascontiguousarray(t.detach().cpu().numpy(), dtype=np.float32)
        a.tofile(out / f"{name}.bin")
        man["bins"][name] = {"shape": list(a.shape), "dtype": "float32"}
        return a

    import nemo.collections.asr as nemo_asr

    m = nemo_asr.models.EncDecClassificationModel.from_pretrained(MODEL).eval().cpu()
    pre = m.preprocessor

    man["labels"] = list(m.cfg.labels)
    man["preprocessor"] = json.loads(json.dumps(dict(m.cfg.preprocessor), default=str))

    # Dump every preprocessor buffer rather than reaching for known attribute
    # paths -- torchaudio moves them between versions, and this way the mel
    # filterbank ships as data so the frontend never reimplements htk-vs-slaney.
    for n, b in pre.named_buffers():
        if b.ndim >= 1:
            dump(f"buf_{n.replace('.', '_')}", b)

    with torch.no_grad():
        sigs = ref_signals()
        feats = {}
        for name, sig in sigs.items():
            f, _ = pre(input_signal=sig[None], length=torch.tensor([WINDOW]))
            feats[name] = f.transpose(1, 2).contiguous()
            dump(f"ref_{name}_audio", sig)
            dump(f"ref_{name}_feat", feats[name])

        wrapped = KWS(m).eval()
        example = feats["noise"]
        torch.onnx.export(
            wrapped, example, str(out / "kws.onnx"),
            input_names=["features"], output_names=["probabilities"],
            dynamic_axes={"features": {0: "batch"}, "probabilities": {0: "batch"}},
            opset_version=17, do_constant_folding=True, dynamo=False,
        )
        man["feature_shape"] = list(example.shape)

        sess = ort.InferenceSession(str(out / "kws.onnx"), providers=["CPUExecutionProvider"])
        worst = 0.0
        for name, f in feats.items():
            ref = wrapped(f)
            got = sess.run(None, {"features": f.numpy()})[0]
            worst = max(worst, float(np.abs(ref.numpy() - got).max()))
            assert np.allclose(got.sum(axis=-1), 1.0, atol=1e-6)
            dump(f"ref_{name}_probabilities", ref)

    man["onnx_vs_torch_max_abs_err"] = worst
    (out / "manifest.json").write_text(json.dumps(man, indent=2))

    assert worst < 1e-4, f"ONNX diverges from PyTorch: {worst}"
    print(f"ok  {out}/kws.onnx  {man['feature_shape']} -> [1, {len(man['labels'])}]")
    print(f"    labels: {man['labels']}")
    print(f"    onnx vs torch: {worst:.2e}")


if __name__ == "__main__":
    main()
