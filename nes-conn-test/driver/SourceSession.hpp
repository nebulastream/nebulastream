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

/// The `--mode=source` session of `conntest-harness`, which the runner steps one command at a time.
/// Commands are `BIND`, `OPEN`, `FILL <n_rows> <n_bytes>` and `CLOSE`; each gets one JSON reply line and EOF ends the session.
/// Every OPEN builds a new connector from the once-bound descriptor, as the engine does,
/// so CLOSE then OPEN never relies on reopening in place.
/// The reply shapes are defined in `conntest_runner/protocol.py`.

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <optional>

namespace NES::ConnTest
{

inline constexpr std::size_t DEFAULT_SOURCE_BUFFER_SIZE = 4096;
inline constexpr std::size_t DEFAULT_SOURCE_BUFFER_COUNT = 8;
inline constexpr std::chrono::milliseconds DEFAULT_SOURCE_STEP_TIMEOUT{30'000};

class ControlChannel;

struct SourceSessionOptions
{
    std::filesystem::path configFile; ///< --config=
    std::size_t bufferSize{DEFAULT_SOURCE_BUFFER_SIZE}; ///< --buffer-size=
    std::size_t bufferCount{DEFAULT_SOURCE_BUFFER_COUNT}; ///< --buffer-count=
    std::chrono::milliseconds stepTimeout{DEFAULT_SOURCE_STEP_TIMEOUT}; ///< --step-timeout-ms=
    /// --observed-path=: each FILL overwrites this file with the bytes that the source delivered during that step.
    /// The runner reads it back and compares the rows with the ones it seeded.
    std::optional<std::filesystem::path> observedPath;
};

/// Returns false on a channel error and true when the channel reaches EOF.
bool runSourceSession(const SourceSessionOptions& options, ControlChannel& channel);

}
