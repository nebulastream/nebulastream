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

#include <ControlChannel.hpp>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <unistd.h>
#include <sys/socket.h>
/// `ssize_t` comes from <sys/types.h> on POSIX; clang-tidy doesn't see the
/// symbol mapping and flags the include as unused.
/// NOLINTNEXTLINE(misc-include-cleaner)
#include <sys/types.h>
#include <sys/un.h>

namespace NES::ConnTest
{

std::optional<ControlChannel> ControlChannel::connect(std::string_view path)
{
    if (path.size() >= sizeof(sockaddr_un::sun_path))
    {
        std::cerr << "conntest-harness: control-sock path too long: " << path << "\n";
        return std::nullopt;
    }

    const int fileDescriptor = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fileDescriptor < 0)
    {
        /// NOLINTNEXTLINE(concurrency-mt-unsafe): conntest-harness is single-process; the control channel is not shared across threads.
        std::cerr << "conntest-harness: socket(AF_UNIX) failed: " << std::strerror(errno) << "\n";
        return std::nullopt;
    }

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay): sockaddr_un::sun_path is a C array; the kernel ABI takes the decayed pointer.
    std::memcpy(addr.sun_path, path.data(), path.size());
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index): the early `if (path.size() >= sizeof(sun_path)) return std::nullopt;` bounds the index.
    addr.sun_path[path.size()] = '\0';

    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): the POSIX socket API takes a generic `sockaddr*`; bridging a typed `sockaddr_un` to it requires the cast.
    while (::connect(fileDescriptor, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
    {
        if (errno == EINTR)
        {
            continue;
        }
        /// NOLINTNEXTLINE(concurrency-mt-unsafe): conntest-harness is single-process; the control channel is not shared across threads.
        std::cerr << "conntest-harness: connect(" << path << ") failed: " << std::strerror(errno) << "\n";
        ::close(fileDescriptor);
        return std::nullopt;
    }
    return ControlChannel{fileDescriptor};
}

ControlChannel::~ControlChannel()
{
    if (fileDescriptor >= 0)
    {
        ::close(fileDescriptor);
    }
}

ControlChannel::ControlChannel(ControlChannel&& other) noexcept : fileDescriptor(other.fileDescriptor), buffer(std::move(other.buffer))
{
    other.fileDescriptor = -1;
}

ControlChannel& ControlChannel::operator=(ControlChannel&& other) noexcept
{
    if (this != &other)
    {
        if (fileDescriptor >= 0)
        {
            ::close(fileDescriptor);
        }
        fileDescriptor = other.fileDescriptor;
        buffer = std::move(other.buffer);
        other.fileDescriptor = -1;
    }
    return *this;
}

std::optional<std::string> ControlChannel::readLine()
{
    while (true)
    {
        if (const auto newlinePos = buffer.find('\n'); newlinePos != std::string::npos)
        {
            std::string line = buffer.substr(0, newlinePos);
            buffer.erase(0, newlinePos + 1);
            if (!line.empty() && line.back() == '\r')
            {
                line.pop_back();
            }
            return line;
        }

        /// Transport-level staging size for one ::read() — how many bytes we
        /// pull off the socket per syscall before scanning for a line break.
        /// Independent of the engine's `--buffer-size` (TupleBuffer) option;
        /// command/reply lines are short, so this only bounds syscall count.
        constexpr std::size_t readChunkSize = 4096;
        std::array<char, readChunkSize> chunk{};
        /// NOLINTNEXTLINE(misc-include-cleaner): ssize_t is provided by <sys/types.h> (above); clang-tidy doesn't see the symbol mapping.
        const ssize_t numberOfBytes = ::read(fileDescriptor, chunk.data(), chunk.size());
        if (numberOfBytes > 0)
        {
            buffer.append(chunk.data(), static_cast<std::size_t>(numberOfBytes));
            continue;
        }
        if (numberOfBytes < 0 && errno == EINTR)
        {
            continue;
        }
        /// EOF (n == 0) or a hard read error: drop any unterminated
        /// trailing bytes — the runner only ever sends whole lines — and
        /// signal end-of-session.
        return std::nullopt;
    }
}

bool ControlChannel::writeLine(std::string_view line) const
{
    std::string framed;
    framed.reserve(line.size() + 1);
    framed.append(line);
    framed.push_back('\n');

    std::size_t off = 0;
    while (off < framed.size())
    {
        /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic,misc-include-cleaner): partial-write loop; ssize_t is from <sys/types.h>.
        const ssize_t numberOfBytes = ::write(fileDescriptor, framed.data() + off, framed.size() - off);
        /// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic,misc-include-cleaner)
        if (numberOfBytes > 0)
        {
            off += static_cast<std::size_t>(numberOfBytes);
            continue;
        }
        if (numberOfBytes < 0 && errno == EINTR)
        {
            continue;
        }
        return false;
    }
    return true;
}

}
