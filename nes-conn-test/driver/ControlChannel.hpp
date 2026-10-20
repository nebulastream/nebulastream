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

#include <optional>
#include <string>
#include <string_view>

/// Line transport between the Python runner and the C++ harness: one command line in, one JSON reply line out, until the runner closes its end.
/// Example turn: `OPEN` -> `{"ok":true,"phase":"open"}`.
/// The harness connects to the AF_UNIX path from `--control-sock` itself, not through an inherited fd,
/// because a debugger wrapper such as `lldb-server gdbserver` closes an inferior's inherited non-stdio fds
/// and the runner does not need to be the harness's parent.

namespace NES::ConnTest
{

class ControlChannel
{
public:
    /// Logs a failure to stderr and returns nullopt.
    static std::optional<ControlChannel> connect(std::string_view path);

    ~ControlChannel();
    ControlChannel(const ControlChannel&) = delete;
    ControlChannel& operator=(const ControlChannel&) = delete;
    /// Move construction is needed to return a channel in an optional.
    /// Move assignment has no caller, so it stays deleted.
    ControlChannel(ControlChannel&& other) noexcept;
    ControlChannel& operator=(ControlChannel&&) = delete;

    /// Strips a trailing '\r'.
    /// EOF and a read error both return nullopt, which ends the session.
    std::optional<std::string> readLine();

    [[nodiscard]] bool writeLine(std::string_view line) const;

private:
    explicit ControlChannel(const int socketFd) : fileDescriptor(socketFd) { }

    int fileDescriptor;
    std::string buffer; ///< Bytes read past the last returned line.
};

}
