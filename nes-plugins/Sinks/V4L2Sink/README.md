# NebulaStream virtual camera

The Linux `V4L2` sink publishes individual frame tuples to an existing
[v4l2loopback](https://github.com/v4l2loopback/v4l2loopback) output device.
Applications can then open that device as a local webcam.

## Set up the virtual camera

Install your distribution's v4l2loopback package and matching kernel headers.
On Debian/Ubuntu, the packages are `v4l2loopback-dkms` and `v4l2loopback-utils`.
Create an unused output device, separate from the physical camera:

```sh
sudo modprobe v4l2loopback video_nr=10 card_label="NebulaStream" exclusive_caps=1
```

The worker needs read/write access to `/dev/video10`, usually through membership
in the `video` group. If v4l2loopback is already loaded, use its device management
tools to create a device or select an existing unused output device.

The sink opens the device; it does not install or load kernel modules.
With `exclusive_caps=1`, webcam applications see the capture device after a
producer attaches. Start the query and let the first frame arrive before
selecting **NebulaStream** in your application's camera picker.

## Run the example

Build the Linux worker with the new sink and start it from the repository root:

```sh
cmake-build-debug-libstdcxx-none/nes-single-node-worker/nes-single-node-worker -- \
  --grpc=127.0.0.1:8080 --data_address=127.0.0.1:9090
```

Then submit the topology:

```sh
cmake-build-debug-libstdcxx-none/nes-frontend/apps/nes-cli \
  -t nes-plugins/Sinks/V4L2Sink/examples/query.yaml start
```

The example forwards `/dev/video0` to `/dev/video10` using YUYV at 320×240.
Adjust the physical camera path, resolution, and frame rate to suit your camera.
You can inspect the output with a V4L2 viewer, for example:

```sh
ffplay -f v4l2 -i /dev/video10
```

## Send your own frames

Use `type: V4L2` and `output_format: NATIVE` on a sink with exactly this ordered,
non-nullable schema, matching the existing V4L2 source:

| Field | Type | Meaning |
| --- | --- | --- |
| TIMESTAMP | UINT64 | Source timestamp; frames older than the last written timestamp are dropped |
| WIDTH | UINT64 | Frame width in pixels |
| HEIGHT | UINT64 | Frame height in pixels |
| PIXEL_FORMAT | UINT64 | Numeric V4L2 FourCC, e.g. 1448695129 for YUYV |
| IMAGE | VARSIZED | Binary bytes of one complete frame |

Each accepted tuple writes one frame, including when a buffer contains several tuples.
The timestamp occupies the first eight bytes of each native tuple, separate from
the image bytes in its child buffer.
Images produced by a UDF work too: replace `IMAGE` with the resulting bytes and
set the metadata to match. Base64 text must be decoded before reaching this sink.

The first tuple sets the output width, height, and pixel format. These must stay
constant until the query stops. The sink writes the bytes as supplied, so any
resize, JPEG decoding, or color conversion belongs in the query before the sink.
Raw image bytes must match the device's negotiated frame size and row layout.
Encoded images such as MJPG are accepted only if the device advertises that
format; the consuming application must support it too.

Frames are published when tuples arrive. `frame_rate` requests the nominal V4L2
device rate; it does not pace a batch or reorder tuples by timestamp. Use a live
source or pace the producer for real-time playback. Concurrent query processing
may change arrival order. The sink drops any tuple with a timestamp strictly
less than the last successfully written frame, including across buffers and
worker threads. Equal timestamps are accepted, and the first timestamp may be
zero. This ordering state resets when the sink starts; timestamps must use a
common time base and must not reset during a stream.

If you want the loopback driver to repeat the latest
frame during gaps, enable its
[`sustain_framerate` control](https://github.com/v4l2loopback/v4l2loopback#attributes):

```sh
v4l2-ctl -d /dev/video10 -c sustain_framerate=1
```

Sink configuration:

| Key | Default | Meaning |
| --- | --- | --- |
| device | /dev/video10 | Existing V4L2 output device |
| frame_rate | 30 | Positive nominal frame rate requested from the driver |
| poll_timeout_ms | 1000 | Positive maximum wait for a frame write when the device is busy |

A device error, write timeout, unsupported format, or invalid frame fails the
query with a diagnostic. The sink closes its device when stopped or destroyed.

The sink also writes diagnostics directly to stderr: device startup, negotiated
format and stride, errors, and the total number of frames received, written, and dropped
at shutdown. Frame details are logged for the first three frames and then at
most once per second, including the timestamp, image size, and successful write.
Out-of-order drops are logged separately for the first three drops and then at
most once per second, with the rejected timestamp, last written timestamp, and
cumulative drop count.
For tightly packed YUYV, the logs include luma minimum, maximum, and mean.
`uniform_black=true` means every pixel has Y=16 and neutral chroma (U/V=128),
which matches the face-alignment query's black fallback. This identifies black
input to the sink; the sink cannot determine whether face detection or another
upstream operation produced it.
