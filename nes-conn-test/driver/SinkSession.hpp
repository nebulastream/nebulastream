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

/// `--mode=sink` stepper session for `conntest-harness`.
///
/// The sink-side analog of SourceSession: a command/reply loop the runner
/// drives one step at a time:
///
///   BIND | START [<offset>] | WRITE <n> | STOP
///
/// The config is bound once (lazily) and cached; START lowers the
/// descriptor to a fresh Sink and start()s it — an optional <offset> resumes
/// WRITE at that buffer, skipping ones a prior (crashed) session already
/// committed (the sink-side analog of a source resuming its backend read
/// position) — WRITE packs and concurrently drains the next n units of the
/// input through the sink, STOP runs the repeat-aware stop loop. Each command
/// produces one JSON reply line; EOF ends the session.

#include <chrono>
#include <cstddef>
#include <filesystem>

namespace NES::ConnTest
{

inline constexpr std::size_t DEFAULT_SINK_THREADS = 4;
inline constexpr std::size_t DEFAULT_SINK_BUFFER_COUNT = 64;
inline constexpr std::size_t DEFAULT_SINK_BUFFER_SIZE = 65'536;
inline constexpr std::chrono::milliseconds DEFAULT_SINK_STEP_TIMEOUT{30'000};

class ControlChannel;

struct SinkSessionOptions
{
    std::filesystem::path configFile; ///< --config=
    std::filesystem::path inputPath; ///< --input-path= (formatted bytes written THROUGH the sink)
    std::size_t threads{DEFAULT_SINK_THREADS}; ///< --threads= (concurrent drain threads)
    std::size_t bufferCount{DEFAULT_SINK_BUFFER_COUNT}; ///< --buffer-count=
    std::size_t bufferSize{DEFAULT_SINK_BUFFER_SIZE}; ///< --buffer-size=
    std::chrono::milliseconds stepTimeout{DEFAULT_SINK_STEP_TIMEOUT}; ///< --step-timeout-ms=
};

/// Run the sink-mode command loop over `channel` until EOF. Returns true on a
/// graceful (EOF) exit, false on a hard channel error.
bool runSinkSession(const SinkSessionOptions& options, ControlChannel& channel);

}
