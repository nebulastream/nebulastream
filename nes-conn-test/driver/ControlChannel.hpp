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

/// Bidirectional, newline-delimited control channel for the stepper
/// protocol. The harness `connect()`s to the AF_UNIX path the runner
/// passes via `--control-sock`; the runner reads JSON reply lines and
/// writes command lines.
///
/// Wire protocol (this class is the transport only — it frames lines and
/// knows nothing about their contents). Each turn is exactly one request
/// line and one reply line, both '\n'-terminated; the session loop runs
/// turns until the runner closes its end (EOF):
///
///   runner (control socket)                 harness (this process)
///   -----------------------                 ----------------------
///     "BIND\n"                     ────────▶  readLine() → dispatch()
///     (source: BIND | OPEN |                  run one step
///      FILL <n> | CLOSE;
///      sink: BIND | START |
///      WRITE <n> | STOP)
///                                  ◀────────  writeLine(reply)
///     reads one JSON reply line               {"ok":true,"phase":"bind"}  (success)
///                                             {"ok":false,"phase":...,"code":...,
///                                              "name":...,"message":...}        (failure)
///     ... repeat per command ...
///     close()                      ────────▶  readLine() → nullopt → loop ends
///
/// The command verbs and reply field shapes live in SourceSession.hpp /
/// SinkSession.hpp (the dispatch bodies) and Report.hpp (`errorReplyLine`);
/// `conntest_runner/protocol.py` is the runner-side mirror.
///
/// Why a socket the harness opens itself rather than the old inherited
/// fd3/fd4 pair: `lldb-server-19 gdbserver` closes the inferior's
/// inherited non-stdio fds, which a one-byte READY/GO handshake dodged
/// with `--skip-handshake`. A live, multi-command session needs the
/// channel open throughout — there is no one-shot dodge — and a socket
/// the inferior opens itself is not an inherited fd, so lldb-server
/// leaves it alone. It also drops the pytest-must-be-the-parent
/// constraint `pass_fds` imposed.

namespace NES::ConnTest
{

class ControlChannel
{
public:
    /// Connect to a listening AF_UNIX stream socket at `path`. Returns
    /// nullopt (after logging to stderr) on any socket/connect failure.
    static std::optional<ControlChannel> connect(std::string_view path);

    ~ControlChannel();
    ControlChannel(const ControlChannel&) = delete;
    ControlChannel& operator=(const ControlChannel&) = delete;
    /// Move-construction is how `connect()`'s by-value result lands in the
    /// caller's optional. Move-*assignment* has no caller (sessions take the
    /// channel by reference and nothing re-seats one), so it stays deleted
    /// rather than carrying dead fd-swap logic.
    ControlChannel(ControlChannel&& other) noexcept;
    ControlChannel& operator=(ControlChannel&&) = delete;

    /// Read one '\n'-terminated command line (terminator stripped, a
    /// trailing '\r' tolerated). Returns nullopt on EOF or read error —
    /// the session loop treats either as a graceful shutdown signal.
    std::optional<std::string> readLine();

    /// Write `line` followed by a single '\n', retrying short writes and
    /// EINTR. Returns false on a hard write error.
    [[nodiscard]] bool writeLine(std::string_view line) const;

private:
    explicit ControlChannel(const int socketFd) : fileDescriptor(socketFd) { }

    int fileDescriptor;
    std::string buffer; ///< bytes read past the last consumed line
};

}
