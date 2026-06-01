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

/// `--mode=source` stepper session for `conntest-harness`.
///
/// Executes one step of a long-lived command/reply loop orchestrated by the runner:
///
///   VALIDATE | OPEN | FILL <n> | DRAIN | CLOSE
///
/// The config is bound once (lazily, on the first VALIDATE/OPEN) and
/// cached; each OPEN builds a *fresh* connector from that descriptor, so
/// an in-process CLOSE→OPEN is a new instance on the same descriptor
/// (mirrors a SourceThread; avoids assuming reopen-in-place). Each command
/// produces exactly one JSON reply line; EOF on the channel ends the
/// session. See `conntest_runner/protocol.py` for the reply shapes.

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
    /// --observed-path= — when set, each FILL/DRAIN (over)writes the bytes
    /// it observed during that step here. RowModel reads it back to decode
    /// rows for a multiset compare; ByteModel compares via the reply sha and
    /// can omit it.
    std::optional<std::filesystem::path> observedPath;
    /// --native-row-width= — native tuple width for NATIVE-formatter sources
    /// whose fillTupleBuffer reports a *tuple* count via withBytes(). The
    /// quota is counted in those native units; the observed byte slice is
    /// `tuples * width`. Supplied by RowModel from its schema. Absent for
    /// byte-stream sources, where withBytes() is already a byte count.
    std::optional<std::size_t> nativeRowWidth;
};

/// Run the source-mode command loop over `channel` until EOF. Returns true on
/// a graceful (EOF) exit, false on a hard channel error.
bool runSourceSession(const SourceSessionOptions& options, ControlChannel& channel);

}
