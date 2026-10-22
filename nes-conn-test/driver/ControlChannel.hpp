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

#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>

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
    /// Throws boost::system::system_error when the path is longer than `sun_path` or the connect fails.
    /// The io_context cannot move, so neither can the channel: it is constructed where it lives, e.g. with `std::optional::emplace`.
    explicit ControlChannel(std::string_view path);

    /// Strips a trailing '\r'.
    /// EOF and a read error both return nullopt, which ends the session.
    std::optional<std::string> readLine();

    [[nodiscard]] bool writeLine(std::string_view line);

private:
    boost::asio::io_context ioContext;
    boost::asio::local::stream_protocol::socket socket{ioContext}; ///< Declared after its context, so destroyed before it.
    std::string buffer; ///< Bytes read past the last returned line.
};

}
