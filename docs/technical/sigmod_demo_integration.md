# Integrating the SIGMOD Demo into Main

This document plans how the multi-modal SIGMOD demo moves from its long-lived demo branches onto `main`.
It describes a minimal first milestone, the follow-up milestones, what we do not port, and where each
commit of the demo branch goes. It is the basis for the epic and its sub-issues.

## The demo today

The demo monitors a patient with a thermal/RGB camera pair, a microphone and a patient monitor, on two
Raspberry Pis (edge and hub). It shows the camera image with the face outlined and its temperature, the
detected keywords, the vital signs, and alerts that combine them.

| Part | Where | Notes |
| --- | --- | --- |
| Engine | branch `demo-rebase-11-16-2026` (40 commits on top of `main` at `3bcb67517e`) | The deployment still pins images of `demo-rebase-10-29-2026`; both carry the same features. |
| arm64 cross build | branch `ls/demo-rebase-10-29-2026-arm64` | 8 commits that are not on the 11-16 branch. |
| Application | [ls-1801/nes-sigmod25-demo](https://github.com/ls-1801/nes-sigmod25-demo) at `eb1329f` | 30 queries in `demo-deploy/templates/topology.yaml.j2`, Codon UDFs in `python_udfs/demo_udfs.py`, ONNX models (YuNet face detection, keyword spotting), patient-monitor and lab-value generators, `nes-cli` scripts. |
| UI | `nebulastream-ui-SIGMOD-Demo`, branch `ls/ui-updates` | Reads everything over MQTT (WebSocket). |

Data flow of the demo application (arrows between queries are pipes):

```
video ──> video_frames ──> thermal_face (YuNet) ──> UI video
                               │
pm_hr, pm_sysbp ──> vitals ────┴──> shock alert
microphone ──> audio_pipe ──> UI waveform, audio windows ──> keyword spotting ──> UI labels, speech alert
MQTT source/live/* ──> pass-through queries ──> MQTT sink/live/*
Process sources (CPU, temperature, memory, network of the Pis) ──> MQTT sink/live/system_*
```

Some demo code has newer versions on other branches. We land those, not the copies on the demo branch.

| Feature | Newer version |
| --- | --- |
| CASE WHEN, stream-table and ASOF joins, record-aware B-tree | `ls/nexmark-coverage-clean` (Nexmark epic #2332) |
| Pipe source and sink | `pipe-on-main` (#2321) |
| Python and Codon UDFs | `claude/python-udfs-split` (UDF epic #2158) |
| `Image` and `ThermalFrame` data types | `extensible-data-types-poc` (#572, design document #1597) |

## Principles

1. **Plugins.** New functionality uses the plugin registries (sources, sinks, functions, aggregations,
   data types). Core changes are the exception and need a reason.
2. **Typed values.** Images, audio frames and tensors are data type plugins on top of the extensible data
   types (#572), not VARSIZED byte blobs whose meaning lives in the query.
3. **Devices over their native protocol.** NES reads a device directly, e.g. a GigE Vision camera or an
   ALSA microphone. There is no broker between the device and NES. A new source is justified where NES does
   not support the protocol yet.
4. **ConnTest for connectors.** Every new source or sink gets a ConnTest suite.
5. **No hardware needed.** The demo runs against simulated devices that replay recordings, in CI and on a
   laptop.
6. **Coordinator as the control plane.** The demo application submits sources, models and queries through
   the coordinator's REST server (#1577, #2245) instead of `nes-cli`. The demo branch's CLI changes are not
   ported.

## Milestone 1: minimal demo (thermal face detection)

One worker reads the camera pair over GigE Vision. For every thermal frame, it takes the latest RGB frame,
finds faces on it with YuNet, and sends the thermal frame with the face outlined and the face's maximum
temperature to the UI.

The camera delivers thermal (MONO16) and RGB (YUYV) frames interleaved on one stream, and only one client
can control it. One query therefore opens the camera and writes into a pipe, and the face detection query
reads the pipe twice:

```sql
-- video_stream: the camera into a pipe
SELECT TIMESTAMP, WIDTH, HEIGHT, PIXEL_FORMAT, IMAGE FROM video INTO video_frames;

-- thermal_face: pair each thermal frame with the latest RGB frame, detect faces, report the face temperature
SELECT thermal_timestamp AS timestamp,
       to_base64(thermal_jpg_with_face(thermal_image, face)) AS image,
       face_max_celsius(thermal_image, face) AS max_temp
FROM (SELECT thermal_timestamp, thermal_image, yunet_face(packedFaces, rgb_image) AS face
      FROM MODEL_INFERENCE(yunet, (
          SELECT thermal_timestamp, thermal_image, rgb_image, yunet_input(rgb_image) AS tensor
          FROM (SELECT timestamp AS thermal_timestamp, image AS thermal_image
                FROM video_frames WHERE PIXEL_FORMAT = 17825799)
               ASOF JOIN (SELECT timestamp AS rgb_timestamp, align_to_thermal(image) AS rgb_image
                          FROM video_frames WHERE PIXEL_FORMAT = 34603058)
               TIME(thermal_timestamp, rgb_timestamp))))
INTO ui_video;
```

The function names are placeholders. `video_frames` is a pipe sink in the first query and a pipe source in
the second, and `ui_video` is an MQTT sink with JSON output. Compared with the demo application, the minimal
demo drops the RGB image that is shown when no face is found (and with it CASE WHEN). It also writes to MQTT
directly instead of through a second pipe, which only the shock alert needs.

### What it needs

Simplicity: 1 = trivial, 5 = very hard.

| Need | Path | Tracked in | Simplicity |
| --- | --- | --- | --- |
| GigE Vision camera source | The demo branch's `VideoSource` (aravis) as a source plugin behind a build feature. Camera id, the Fluke stream selector, packet size and byte order become configuration; frames stay within a memory budget. Commits `b320dccd2b` (source part), `e8e0bfe192` (reconnect). | new | 4 |
| Simulated camera | A simulated GigE Vision camera that replays recorded thermal and RGB frames. Builds on `4ccf2a81b5`, which runs aravis's fake camera with the Fluke selector but only produces synthetic MONO8 frames. Used by ConnTest, the end-to-end test and local runs. | new | 3 |
| Recordings | A short recording of the camera pair with and without faces, raw MONO16 and YUYV with camera timestamps. `tools/thermal_calibration.py capture` in the application repo is a starting point. Needs the camera. | new | 1 |
| ConnTest suite for the camera source | Scenarios against the simulated camera: frames arrive, the camera stops and comes back, no camera, close while reading. | new | 3 |
| One camera, two readers | Pipe source and sink, including the multi-origin fix in the same commit. | #2321 | 3 |
| Pair thermal and RGB frames | ASOF join (`TIME(l, r)`, no equality condition). The Nexmark prototype's version keeps rows in the record-aware B-tree; the demo branch's version (`82cc43d285`) scans paged vectors and is the fallback. | new, after #2344 | 4 |
| Typed frames | `Image` and `ThermalFrame` data type plugins with a pixel `VECTOR`. A frame is far larger than a tuple buffer, so it needs VECTOR, not FIXEDSIZED. | new, after #572 (STRUCT, VECTOR) | 2 |
| Image functions | Function plugins: align an RGB frame to the thermal frame's pixel grid, YuNet input tensor, YuNet output to a face rectangle, maximum temperature inside a rectangle (calibration as configuration), colour-mapped JPEG with the face outlined. A missing face gives no rectangle and 0 °C. The logic comes from `demo_udfs.py` and the demo branch's ImageManip plugin. OpenCV (core, imgproc, imgcodecs) sits behind a build feature. | new | 3 |
| Face detection | `MODEL_INFERENCE` with the YuNet ONNX model. | on `main` | – |
| Results to the UI | `TO_BASE64`, JSON output and the MQTT sink are on `main`. The sink must not publish empty payloads, because an empty retained message clears the UI's image (`8cc6af374a`). | new (fix) | 1 |
| Control plane | The coordinator and its REST server. The application creates the camera source, the YuNet model, the pipe and the MQTT sink, starts `video_stream`, waits until it is running (`wait=Running`), and then starts `thermal_face`. | #1577, #2245, new (application) | 2 |
| End-to-end test | Both queries against the simulated camera in CI, checking face rectangles and temperatures on the recordings. | new | 3 |

### Order of work

1. Independent of everything else: the recordings, the MQTT sink fix, the build features for aravis and
   OpenCV, the pipe PR (#2321), and an issue for the ASOF join.
2. Four tracks in parallel:
   - camera source, simulated camera and ConnTest suite;
   - image functions, on the data types from #572;
   - ASOF join, after the B-tree (#2344);
   - REST server (#2245), after the coordinator (#1577).
3. The application on the REST server, and the end-to-end test.

The critical path runs through #572 (STRUCT and VECTOR), #2344 and #2245.

### Open questions

- Can aravis's fake camera replay recorded MONO16 and YUYV frames, or do we need our own fill routine?
- #572 plans VECTOR after STRUCT and FIXEDSIZED. If VECTOR is not ready, does the first cut keep the demo's
  VARSIZED `IMAGE` with `WIDTH`, `HEIGHT` and `PIXEL_FORMAT` columns and migrate later?
- A pipe consumer that starts before its producer fails today (`No pipe sink registered`). Should the pipe
  source wait for its producer instead, and should it check the producer's schema? Both belong to #2321.
- How do we test pipes? Both ends run inside one worker, so ConnTest fits them poorly. Gtests and
  multi-query systests are the alternative.
- The UI reads over MQTT, so the output side keeps a broker. The input side has none.
- The aravis dependency (glib, libxml2, libusb) is heavy and must stay optional.

## Follow-ups

### Milestone 2: RGB fallback and shock alert

- Show the aligned RGB frame when no face is found: CASE WHEN (#2361, the Nexmark prototype's version
  `1c4399af2e` and `5c7a7adf05`, which adds optional ELSE and single-exit tracing) and an RGB-to-JPEG
  function.
- Vital signs: the patient-monitor generator publishes heart rate and blood pressure over MQTT, which is
  its native protocol, so the MQTT source on `main` reads it. `vitals` pairs heart rate with the latest
  blood pressure (ASOF join from milestone 1).
- Shock alert: `thermal_face` writes into a pipe that both the UI query and the alert read. The alert
  joins warm faces with the latest vitals (ASOF) and reduces them to one alert per second (tumbling window,
  COUNT).
- The lab values and the MQTT pass-through queries of the application come with this milestone if the UI
  needs them.

### Milestone 3: audio

- Microphone: the demo branch's ALSA `AudioSource` (`b320dccd2b`) as a source plugin with a ConnTest suite.
  It reads S16_LE mono at 16 kHz and follows the ALSA hardware clock. A simulated microphone (ALSA loopback
  or a file-backed PCM) replays recordings; whether either works inside the ConnTest containers needs
  checking.
- `AudioFrame` data type plugin with a sample `VECTOR`, so a tuple carries a frame instead of a single sample
  (the demo branch emits one tuple per sample).
- One-second windows: the demo builds them with ARRAY_AGG over a sliding window (`4d93c423dc`). ARRAY_AGG
  becomes an aggregation plugin after #2155 with a `VECTOR` result instead of raw bytes. Its ordering is only
  exact when buffers do not overlap in time, which needs documenting or fixing.
- Keyword spotting: MFCC and tensor reductions (argmax, max, absolute max) as function plugins, and
  `MODEL_INFERENCE` on a typed tensor (relates to #2327 and #2329).
- Waveform to the UI and the speech alert (GROUP BY, tumbling window, MAX).

### Milestone 4: edge and hub on two Raspberry Pis

- Network: report send errors and stop the worker from aborting in `addChildBuffer` (`65a0ff6a26`), and allow
  frames up to 128 MiB, as a named limit or configuration (`e2e727ff3c`). Camera frames cross from edge to
  hub.
- arm64 images: the cross build on `ls/demo-rebase-10-29-2026-arm64`, with one CI job so it does not rot.
  Its fix for the nautilus port's `MLIR_DIR`/`LLVM_DIR` (`e3f71848b9`) can land on its own.
- Memory and robustness on the Pis: configurable gRPC thread count (`97baf04c0e`), Emit's debug-only set of
  completed sequences (`6c57727528`, together with #2151), terminate only pipelines that completed startup
  (`6c11030995`), model memory (#2327) and small inference backends (#2325).

### Further, optional

- System metrics of the Pis: the demo branch's Process source (`d428450d0d`, `2a6fc28425`) needs a ConnTest
  suite and a worker-side opt-in, because it runs shell commands.
- UI query panel from the REST server instead of the MQTT status message the application publishes today.
- Latency tracker (`e79b6895ab`, `a6880f8335`, `5574a74d9f`). It also switches successor tasks to the same
  worker thread by default ("continuations"), which needs its own engine PR with a benchmark, and it overlaps
  the query engine's statistic listener and #1949.
- Source thread names with query id and source type (`76bf1c0c21`), with a non-pure `getType()`.
- EXPLAIN prints casts (`845469160b`).
- Codon UDFs as an alternative to the function plugins, through the UDF epic (#2158).

## Not ported

| What | Why |
| --- | --- |
| CLI named queries, lifecycle, pipe rewriting, model-import skip (`b6d1d7f145`, `6ba3dbd407`) | The coordinator stores named queries, their states and imported models; the REST server waits for states. |
| ImageManip plugin as is | About 1.8k lines of code plus a 33k-line vendored Haar cascade, while face detection uses YuNet. It duplicates `TO_BASE64` with OpenSSL and has no tests. Its image and audio code is a source for the new function plugins. |
| V4L2 source and sink | The demo does not use them; they need kernel modules that CI cannot load. |
| LAST aggregation | Unused and unreachable from SQL; its state grows with every newer record. |
| Stream-table join | Unused by the demo; the Nexmark epic covers it (#2353). |
| Sink output ordering (`6f20b27662`) | Unused by the demo; #2132 covers it. Its sequencer ignores the origin id. |
| Inline `PYTHON()` syntax, PyPy bridge | Dropped on the UDF split branch. |
| Profiling instrumentation (`a13872d16c`) | Superseded by the latency tracker and by #2261. |
| jemalloc in the runtime image (`a9f5258107`) | Debugging only. |
| OpenVINO change (`28fcc83556`) | Already on `main`. |
| Demo tools in the engine repository (`31e31b4142`) | Superseded by the application repository. |

## Appendix: commits of `demo-rebase-11-16-2026`

| Commit | Subject | Goes to |
| --- | --- | --- |
| `171a9ed4d6` | build: align toolchains, dependency caching and CI workflows | infrastructure, split: libc++ hardening mode, dev image cache, UBSan arm64 runtime library; its ITT check is dropped |
| `a13872d16c` | profiling: instrument runtime tasks and preserve JIT frame pointers | not ported |
| `76bf1c0c21` | sources: include query identity and source type in thread names | optional |
| `e2e727ff3c` | network: allow frames up to 128 MiB | milestone 4 |
| `65a0ff6a26` | network: report send errors and payload sizes | milestone 4 (a bug fix, can land any time) |
| `4d93c423dc` | aggregation: add LAST and internally page-sorted ARRAY_AGG | ARRAY_AGG in milestone 3; LAST not ported |
| `17a63895d0` | sql: support CASE WHEN expressions | milestone 2, through #2361 |
| `6c11030995` | runtime: only terminate pipelines that completed startup | milestone 4 (can land any time) |
| `82cc43d285` | joins: add stream-table and ASOF execution | ASOF in milestone 1; stream-table join through #2353 |
| `6f20b27662` | sinks: support configurable output ordering | #2132 |
| `90a6c38fa7` | pipe: connect queries with bounded inter-query channels | milestone 1, through #2321 (`pipe-on-main`) |
| `d428450d0d` | sources: add reusable process input | optional |
| `b6d1d7f145` | cli: support named queries, inferred pipes and query lifecycle | not ported (coordinator) |
| `61fdf472ea` | demo: add media dependencies and container packages | milestone 1 (aravis, OpenCV subset, behind build features) |
| `b320dccd2b` | demo: add camera, audio and media function plugins | camera source in milestone 1, audio source in milestone 3, ImageManip not ported as is |
| `31e31b4142` | demo: add topology, viewer, model and recording tools | not ported |
| `a240c48d3a` | feat(codon): inline Python UDFs compiled with Codon, OpenCV/BLAS Codon plugins, V4L2 source and sink | #2158; V4L2 not ported |
| `c423ea334a` | feat(udf): catalog scalar UDFs with CPython and PyPy bridges, Python venv config, BRIDGE selection for inline PYTHON() | #2158 |
| `23bfe8d9e6` | Codon Catalog support | #2158 |
| `a87c4724e3` | Added more tests | #2158 |
| `ea9c5bc11f` | python udf | #2158 |
| `8cc6af374a` | dont send empty mqtt messages | milestone 1 |
| `42a647d9d6` | ci only needs asan/none | infrastructure; the UDF parts with #2158 |
| `e305cd28bf` | ci specifc images | infrastructure |
| `2a6fc28425` | sources: spawn process source commands with posix_spawn | optional (Process source) |
| `26d207f85f` | docker: install the CPython UDF bridge's runtime library | #2158 |
| `6c57727528` | emit: drop the completed-sequence tracking | milestone 4, with #2151 |
| `97baf04c0e` | worker: limit the gRPC server to four threads | milestone 4, as configuration |
| `20ecaf8cbd` | selection: set up the predicate's functions | with the first function plugin that needs setup, otherwise #2158 |
| `fb6e4c5faf` | operators: set up the child operator in the wrapper | as `20ecaf8cbd` |
| `a9f5258107` | docker: add jemalloc with heap profiling to the runtime image | not ported |
| `6ba3dbd407` | cli: skip importing models for status and stop | not ported (coordinator) |
| `e79b6895ab` | Latency Tracker | optional |
| `4bf7cac60c` | test: give the instrumented builds time in the long running e2e tests | with the tests that need it |
| `28fcc83556` | build(openvino): run inference sequentially on the calling thread | not ported (on `main`) |
| `e8e0bfe192` | fix(video-source): Connects to the camera again when its stream stops | milestone 1 |
| `4ccf2a81b5` | test(video-source): Runs the video source against a simulated GigE Vision camera | milestone 1 (simulated camera, ConnTest) |
| `a6880f8335` | fix(scripts): Draws only the traced query instances, with all inputs of their joins | optional (latency tracker) |
| `845469160b` | fix(logical-operators): Explains a cast with its argument and target type | optional |
| `5574a74d9f` | fix(runtime): Describes the operators a logical operator lowers to once | optional (latency tracker) |
