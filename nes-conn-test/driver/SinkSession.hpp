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
/// Commands are `BIND`, `START`, `WRITE <n_buffers>` and `STOP`; each gets one JSON reply line and EOF ends the session.
/// START packs the input's JSONL records into buffers and starts a new sink from the once-bound descriptor.
/// WRITE hands the next buffers to the sink on several threads at once, as the engine's worker threads do.
/// The reply shapes are defined in `conntest_runner/protocol.py`.

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <optional>

namespace NES::ConnTest
{

inline constexpr std::size_t DEFAULT_SINK_THREADS = 4;
inline constexpr std::size_t DEFAULT_SINK_BUFFER_SIZE = 4096;
inline constexpr std::size_t DEFAULT_SINK_BUFFER_COUNT = 64;
/// Below the runner's deadline of 120 s per test, as the source's.
inline constexpr std::chrono::milliseconds SINK_STEP_TIMEOUT{30'000};

class ControlChannel;

struct SinkSessionOptions
{
    std::filesystem::path configFile; ///< --config=
    /// --input-path=: the JSONL records that the sink writes, without which START makes no buffers.
    /// The engine compiles its output formatter into the query, so the sink only receives formatted bytes, and these are the bytes.
    std::optional<std::filesystem::path> inputPath;
    std::size_t threads{DEFAULT_SINK_THREADS}; ///< --threads=
    std::size_t bufferSize{DEFAULT_SINK_BUFFER_SIZE}; ///< --buffer-size=
    std::size_t bufferCount{DEFAULT_SINK_BUFFER_COUNT}; ///< --buffer-count=
};

/// Returns false on a channel error and true when the channel reaches EOF.
bool runSinkSession(const SinkSessionOptions& options, ControlChannel& channel);

}
