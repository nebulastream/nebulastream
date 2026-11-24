/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

/// The `--mode=sink` session of `conntest-harness`, which the runner steps one command at a time.
/// Commands are `BIND`, `LOAD`, `START`, `WRITE <n_buffers>`, `WRITE_EMPTY` and `STOP`.
/// Each command gets one JSON reply line, and EOF ends the session.
/// LOAD packs the input's JSONL records into batches and makes the engine, whose pool is sized for them.
/// One batch becomes one buffer of `WRITE <n_buffers>`: a main buffer, and child buffers for the bytes that do not fit.
/// Every START runs a query in the engine, from a source that reads the batches into a new sink from the once-bound descriptor.
/// WRITE releases the next batches to the source and returns once the sink has taken them.
/// WRITE_EMPTY releases one buffer without tuples in the same way.
/// STOP stops the query, and the engine stops the sink.
/// The reply shapes are defined in `conntest_runner/protocol.py`.

#include <filesystem>
#include <optional>

#include <SessionOptions.hpp>

namespace NES::ConnTest
{

inline constexpr WorkerThreads DEFAULT_SINK_THREADS{4U};
inline constexpr BufferSize DEFAULT_SINK_BUFFER_SIZE{4096U};

class ControlChannel;

struct SinkSessionOptions
{
    std::filesystem::path configFile; ///< --config=
    /// --input-path=: the JSONL records that the sink writes, without which LOAD makes no buffers.
    /// The harness covers sinks with the JSON output format, for which the engine compiles the output formatter into the query.
    /// Such a sink receives formatted bytes, and these are the bytes.
    std::optional<std::filesystem::path> inputPath;
    WorkerThreads threads{DEFAULT_SINK_THREADS}; ///< --threads=: the engine's worker threads
    BufferSize bufferSize{DEFAULT_SINK_BUFFER_SIZE}; ///< --buffer-size=
};

/// Returns false on a channel error and true when the channel reaches EOF.
/// After a step timed out, an engine thread may still be in the sink,
/// so the harness replies with a StepTimeout error and exits with STUCK_EXIT_CODE without unwinding.
bool runSinkSession(const SinkSessionOptions& options, ControlChannel& channel);

}
