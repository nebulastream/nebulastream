# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Scalar UDFs of the camera/audio demo (topology.yaml `functions:`), compiled by Codon (`bridge: codon`).

Replaces the native ImageManip functions. Put this directory on the worker's module search path, e.g. by starting
the worker with NES_UDF_PATH=demo/python_udfs. A VARSIZED value is a `str` holding raw bytes. UDF calls are strict:
a NULL argument gives a NULL result without calling the function.
"""

import cv2
import numpy as np
from math import cos, sin, log, log10, pi, sqrt, exp

JPEG_QUALITY = 90
# BGR colour of the detection rectangle (the old YUV (200, 50, 0))
RECTANGLE_COLOR = (62, 255, 21)
RECTANGLE_THICKNESS = 2


def _encode_jpeg(image):
    ok, jpg = cv2.imencode(".jpg", image, (cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY))
    return str(jpg.data.as_byte(), int(jpg.nbytes))


def _yuyv_to_bgr(image, width, height):
    return cv2.cvtColor(np.frombuffer(image, dtype=np.uint8).reshape((int(height), int(width), 2)), cv2.COLOR_YUV2BGR_YUYV)


def _mono16_to_color(image, width, height):
    mono16 = np.frombuffer(image, dtype=np.uint16).reshape((int(height), int(width)))
    mono8 = cv2.normalize(mono16, None, 0, 255, cv2.NORM_MINMAX, cv2.CV_8U)
    return cv2.applyColorMap(mono8, cv2.COLORMAP_INFERNO)


def _draw_rectangle(frame, face):
    # `face` is a packed rectangle: x, y, width, height as four little-endian uint16 values
    x = int(face & 0xFFFF)
    y = int((face >> 16) & 0xFFFF)
    w = int((face >> 32) & 0xFFFF)
    h = int((face >> 48) & 0xFFFF)
    cv2.rectangle(frame, (x, y), (x + w, y + h), RECTANGLE_COLOR, RECTANGLE_THICKNESS, cv2.LINE_8, 0)


def yuyv_to_jpg(image, width, height):
    """YUYV frame -> JPEG."""
    return _encode_jpeg(_yuyv_to_bgr(image, width, height))


def yuyv_to_jpg_with_rectangle(image, width, height, face):
    """YUYV frame with the detected face outlined -> JPEG."""
    frame = _yuyv_to_bgr(image, width, height)
    _draw_rectangle(frame, face)
    return _encode_jpeg(frame)


def mono16_to_jpg(image, width, height):
    """16-bit thermal frame, min-max normalised and INFERNO colour mapped -> JPEG."""
    return _encode_jpeg(_mono16_to_color(image, width, height))


def mono16_to_jpg_with_rectangle(image, width, height, face):
    """Thermal frame as `mono16_to_jpg`, with the detected face outlined."""
    frame = _mono16_to_color(image, width, height)
    _draw_rectangle(frame, face)
    return _encode_jpeg(frame)


# Calibration of the Fluke RSE600 thermal camera, range 0, one entry per calibration curve segment:
# (u0, u1, u2, start temperature C, end temperature C). Taken from nes-plugins/ImageManip/calibration.bin.
# A raw sensor value (power) p inside a segment's [power(start), power(end)] maps to
# T = (-u1 + sqrt(u1^2 - 4*u2*u0 + 4*u2*p)) / (2*u2).
THERMAL_CALIBRATION = [
    (9.999999717180685e-10, 9.999999717180685e-10, 9.999999717180685e-10, -273.1000061035156, -180.0),
    (781.75390625, 9.191112518310547, 0.0269467830657959, -180.0, -120.0),
    (2787.941650390625, 45.20158767700195, 0.18771547079086304, -120.0, -30.0),
    (3000.0, 56.268104553222656, 0.3209790587425232, -30.0, 0.0),
    (3000.0, 55.76605224609375, 0.3763006627559662, 0.0, 80.0),
    (2778.99267578125, 59.76850891113281, 0.3608023524284363, 80.0, 200.0),
    (-1305.876220703125, 99.38076782226562, 0.2648627758026123, 200.0, 350.0),
]


def _temperature_to_power(temperature, u0, u1, u2):
    # std::round (half away from zero), then the uint16 wrap of the camera's C++ implementation
    power = ((temperature * u2) + u1) * temperature + u0
    rounded = int(power + 0.5) if power >= 0.0 else -int(-power + 0.5)
    return rounded & 0xFFFF


def mono16_to_celsius(power):
    """Raw 16-bit thermal sensor value -> degrees Celsius (NaN outside the calibrated range)."""
    result = float("nan")
    for u0, u1, u2, start, end in THERMAL_CALIBRATION:
        if int(power) >= _temperature_to_power(start, u0, u1, u2) and int(power) <= _temperature_to_power(end, u0, u1, u2):
            discriminant = u1 * u1 - 4.0 * u2 * u0 + 4.0 * u2 * float(power)
            if discriminant >= 0.0:
                result = (-u1 + sqrt(discriminant)) / (2.0 * u2)
    return result


def thermal_roi_max_celsius(image, width, height, face):
    """Hottest pixel inside the face rectangle of a 16-bit thermal frame, in degrees Celsius."""
    mono16 = np.frombuffer(image, dtype=np.uint16).reshape((int(height), int(width)))
    x0 = min(int(face & 0xFFFF), int(width))
    y0 = min(int((face >> 16) & 0xFFFF), int(height))
    x1 = min(x0 + int((face >> 32) & 0xFFFF), int(width))
    y1 = min(y0 + int((face >> 48) & 0xFFFF), int(height))
    maximum = 0
    for row in range(y0, y1):
        for column in range(x0, x1):
            value = int(mono16[row, column])
            if value > maximum:
                maximum = value
    return mono16_to_celsius(maximum)


def thermal_to_jpg(image, width, height):
    """16-bit thermal frame, min-max normalised and INFERNO colour mapped -> JPEG."""
    return _encode_jpeg(_mono16_to_color(image, width, height))


def thermal_to_jpg_with_rectangle(image, width, height, face):
    """INFERNO colour mapped thermal frame with the detected face outlined -> JPEG."""
    frame = _mono16_to_color(image, width, height)
    _draw_rectangle(frame, face)
    return _encode_jpeg(frame)


# --- Face detection with YuNet (nes-systests/testdata/model/face_detection_yunet_2026may_320_packed.onnx) -------------
# yunet_preprocess -> MODEL_INFERENCE(yunet, ...) -> yunet_face_rectangle, see nes-systests/inference/InferModelYuNet.test.

YUNET_INPUT_SIZE = 320
YUNET_STRIDES = (8, 16, 32)
YUNET_MIN_SCORE = 0.6


def yunet_preprocess(image, width, height):
    """YUYV frame -> float32 BGR CHW tensor of the 320x320 YuNet input (raw 0..255 pixel values)."""
    bgr = _yuyv_to_bgr(image, width, height)
    resized = cv2.resize(bgr, (YUNET_INPUT_SIZE, YUNET_INPUT_SIZE), interpolation=cv2.INTER_LINEAR)
    tensor = resized.astype(np.float32).transpose((2, 0, 1)).copy()
    return str(tensor.data.as_byte(), int(tensor.nbytes))


def yunet_face_rectangle(packed_faces, width, height):
    """Packed YuNet output -> best face as a rectangle in pixels of the original (width x height) frame.

    The rectangle is packed as four little-endian uint16 values (x, y, width, height), the format all the rectangle
    helpers above use. 0 means that no face scored at least YUNET_MIN_SCORE.
    """
    total_locations = 0
    for stride in YUNET_STRIDES:
        grid = YUNET_INPUT_SIZE // stride
        total_locations += grid * grid

    values = np.frombuffer(packed_faces, dtype=np.float32)
    if int(values.size) != total_locations * 16:
        raise ValueError("unexpected packed YuNet output size")

    best_score = -1.0
    best_stride = 0
    best_grid = 0
    best_index = 0
    best_bbox_offset = 0
    preceding_locations = 0
    for stride in YUNET_STRIDES:
        grid = YUNET_INPUT_SIZE // stride
        class_offset = preceding_locations
        object_offset = total_locations + preceding_locations
        bbox_offset = 2 * total_locations + 4 * preceding_locations
        for index in range(grid * grid):
            class_score = min(1.0, max(0.0, float(values[class_offset + index])))
            object_score = min(1.0, max(0.0, float(values[object_offset + index])))
            score = sqrt(class_score * object_score)
            if score > best_score:
                best_score = score
                best_stride = stride
                best_grid = grid
                best_index = index
                best_bbox_offset = bbox_offset
        preceding_locations += grid * grid

    if best_score < YUNET_MIN_SCORE:
        return 0

    row = best_index // best_grid
    column = best_index % best_grid
    bbox = best_bbox_offset + best_index * 4
    center_x = (column + float(values[bbox])) * best_stride
    center_y = (row + float(values[bbox + 1])) * best_stride
    box_width = exp(float(values[bbox + 2])) * best_stride
    box_height = exp(float(values[bbox + 3])) * best_stride

    # normalised to the 320x320 input, then scaled to the original frame and clamped to it
    scale_x = float(width) / float(YUNET_INPUT_SIZE)
    scale_y = float(height) / float(YUNET_INPUT_SIZE)
    x0 = max(0, int((center_x - box_width / 2.0) * scale_x))
    y0 = max(0, int((center_y - box_height / 2.0) * scale_y))
    x1 = min(int(width), int((center_x + box_width / 2.0) * scale_x))
    y1 = min(int(height), int((center_y + box_height / 2.0) * scale_y))
    if x1 <= x0 or y1 <= y0:
        return 0
    return x0 | (y0 << 16) | ((x1 - x0) << 32) | ((y1 - y0) << 48)


def max_abs_f32(audio):
    """Largest absolute sample of a float32 buffer."""
    samples = np.frombuffer(audio, dtype=np.float32)
    maximum = 0.0
    for i in range(int(samples.size)):
        value = float(samples[i])
        maximum = max(maximum, -value if value < 0.0 else value)
    return maximum


def argmax_f32(values):
    """Index of the largest element of a float32 buffer (first one on ties)."""
    scores = np.frombuffer(values, dtype=np.float32)
    best = 0
    for i in range(1, int(scores.size)):
        if scores[i] > scores[best]:
            best = i
    return best


def max_f32(values):
    """Largest element of a float32 buffer."""
    scores = np.frombuffer(values, dtype=np.float32)
    best = float(scores[0])
    for i in range(1, int(scores.size)):
        best = max(best, float(scores[i]))
    return best


def audio_to_mfcc(audio):
    """One second of 16 kHz float32 audio -> frame-major [101, 64] float32 MFCC tensor.

    Hann-windowed 512-point DFT of 400-sample frames (stride 160, reflect-101 padded), 64 triangular mel bands,
    log, orthonormal DCT-II. Matches numpy/scipy to ~1e-6.
    """
    samples = np.frombuffer(audio, dtype=np.float32)
    n = 16000
    nfft = 512
    frame_len = 400
    stride = 160
    bins = nfft // 2 + 1
    coeffs = 64
    frames = n // stride + 1
    pad = (nfft - frame_len) // 2

    window = [0.0] * nfft
    for k in range(frame_len):
        window[pad + k] = 0.5 - 0.5 * cos(2.0 * pi * k / frame_len)

    cos_table = [0.0] * nfft
    sin_table = [0.0] * nfft
    for k in range(nfft):
        cos_table[k] = cos(2.0 * pi * k / nfft)
        sin_table[k] = sin(2.0 * pi * k / nfft)

    # triangular mel filterbank, [bins][coeffs]
    mel_points = coeffs + 2
    max_mel = 2595.0 * log10(1.0 + 8000.0 / 700.0)
    edges = [0.0] * mel_points
    for p in range(mel_points):
        mel = max_mel * p / (mel_points - 1)
        edges[p] = 700.0 * (10.0 ** (mel / 2595.0) - 1.0)
    filterbank = [0.0] * (bins * coeffs)
    for b in range(bins):
        freq = 8000.0 * b / (bins - 1)
        for c in range(coeffs):
            rising = (freq - edges[c]) / (edges[c + 1] - edges[c])
            falling = (edges[c + 2] - freq) / (edges[c + 2] - edges[c + 1])
            value = min(rising, falling)
            filterbank[b * coeffs + c] = value if value > 0.0 else 0.0

    # orthonormal DCT-II basis, [coeffs][coeffs]
    dct = [0.0] * (coeffs * coeffs)
    for j in range(coeffs):
        scale = (1.0 / coeffs) ** 0.5 if j == 0 else (2.0 / coeffs) ** 0.5
        for k in range(coeffs):
            dct[j * coeffs + k] = scale * cos(pi * (2 * k + 1) * j / (2.0 * coeffs))

    out = np.zeros(frames * coeffs, dtype=np.float32)
    windowed = [0.0] * nfft
    power = [0.0] * bins
    log_mel = [0.0] * coeffs
    half = nfft // 2
    for f in range(frames):
        start = f * stride
        for i in range(nfft):
            if window[i] == 0.0:
                windowed[i] = 0.0
                continue
            j = start + i - half  # index into the unpadded signal, reflect-101 at the borders
            if j < 0:
                j = -j
            if j >= n:
                j = 2 * (n - 1) - j
            windowed[i] = float(samples[j]) * window[i]
        for b in range(bins):
            re = 0.0
            im = 0.0
            for i in range(pad, pad + frame_len):
                idx = (b * i) % nfft
                re += windowed[i] * cos_table[idx]
                im -= windowed[i] * sin_table[idx]
            power[b] = re * re + im * im
        for c in range(coeffs):
            acc = 0.0
            for b in range(bins):
                acc += power[b] * filterbank[b * coeffs + c]
            log_mel[c] = log(acc + 1e-6)
        for j in range(coeffs):
            acc = 0.0
            for k in range(coeffs):
                acc += log_mel[k] * dct[j * coeffs + k]
            out[f * coeffs + j] = acc
    return str(out.data.as_byte(), int(out.nbytes))