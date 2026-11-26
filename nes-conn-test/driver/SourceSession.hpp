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
/// `FILL 0 0` returns once the connector has entered a read, and stopping a source is bounded by the step timeout.
/// Every OPEN builds a new connector from the once-bound descriptor, as the engine does,
/// so CLOSE then OPEN never relies on reopening in place.
/// The reply shapes are defined in `conntest_runner/protocol.py`.

#include <filesystem>
#include <memory>
#include <optional>

#include <Runtime/AbstractBufferProvider.hpp>
#include <SessionOptions.hpp>

namespace NES::ConnTest
{

inline constexpr BufferSize DEFAULT_SOURCE_BUFFER_SIZE{4096U};
inline constexpr BufferCount DEFAULT_SOURCE_BUFFER_COUNT{8U};

class ControlChannel;

struct SourceSessionOptions
{
    std::filesystem::path configFile; ///< --config=
    BufferSize bufferSize{DEFAULT_SOURCE_BUFFER_SIZE}; ///< --buffer-size=
    BufferCount bufferCount{DEFAULT_SOURCE_BUFFER_COUNT}; ///< --buffer-count=
    /// --observed-path=: each FILL overwrites this file with the bytes that the source delivered during that step.
    /// The runner reads it back and compares the rows with the ones it seeded.
    std::optional<std::filesystem::path> observedPath;
};

/// Returns false on a channel error and true when the channel reaches EOF.
/// Every OPEN's source takes its buffers from the pool, which the caller makes from the flags before the session starts.
/// When a source does not stop within the step timeout, its thread still uses the session,
/// so the harness replies with a StepTimeout error and exits with STUCK_EXIT_CODE without unwinding.
bool runSourceSession(const SourceSessionOptions& options, std::shared_ptr<AbstractBufferProvider> pool, ControlChannel& channel);

}
