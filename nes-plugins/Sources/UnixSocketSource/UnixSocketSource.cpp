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

#include <UnixSocketSource.hpp>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <expected>
#include <format>
#include <iterator>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Util/Files.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(bugprone-throwing-static-initialization,cert-err58-cpp)
const ConfigField<std::string> PATH{Identifier::parse("PATH"), "The path to an existing Unix-domain stream socket."};
/// NOLINTEND(bugprone-throwing-static-initialization,cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> UnixSocketSource::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("UNIX_SOCKET_SOURCE"), PATH);
}

std::expected<UnixSocketSourceConfig, Exception> UnixSocketSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    return UnixSocketSourceConfig{.path = config.get(PATH)};
}

UnixSocketSource::UnixSocketSource(const UnixSocketSourceConfig& config) : path(config.path)
{
}

UnixSocketSource::~UnixSocketSource()
{
    close();
}

void UnixSocketSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    if (path.empty() || path.size() >= sizeof(sockaddr_un{}.sun_path) || path.contains('\0'))
    {
        throw CannotOpenSource("Invalid Unix socket path: {}", path);
    }
    close();
    descriptor = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (descriptor < 0)
    {
        const auto errorMessage = getErrorMessageFromERRNO();
        throw CannotOpenSource("Could not create Unix socket for source: path={}, error={}", path, errorMessage);
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(std::data(address.sun_path), path.c_str(), path.size() + 1);
    /// POSIX connect accepts sockaddr for all address families.
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    if (::connect(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0)
    {
        const auto errorMessage = getErrorMessageFromERRNO();
        close();
        throw CannotOpenSource("Could not connect to Unix socket source: path={}, error={}", path, errorMessage);
    }
}

void UnixSocketSource::close()
{
    if (descriptor >= 0)
    {
        ::close(descriptor);
        descriptor = -1;
    }
}

Source::FillTupleBufferResult UnixSocketSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    PRECONDITION(descriptor >= 0, "Unix socket source was not opened");
    const auto memory = tupleBuffer.getAvailableMemoryArea<char>();
    /// Interrupt an idle read on cancellation without closing/reusing its descriptor concurrently.
    /// The callback is destroyed before close() can release the descriptor on the source thread.
    const std::stop_callback onStop{stopToken, [this] { ::shutdown(descriptor, SHUT_RD); }};
    while (true)
    {
        const auto count = ::recv(descriptor, memory.data(), memory.size(), 0);
        if (count < 0 && errno == EINTR)
        {
            continue;
        }
        if (count < 0)
        {
            const auto errorMessage = getErrorMessageFromERRNO();
            throw CannotReadFromSource("Could not read from Unix socket source: path={}, error={}", path, errorMessage);
        }
        if (count == 0)
        {
            return FillTupleBufferResult::eos();
        }
        const auto numBytesRead = static_cast<size_t>(count);
        totalNumBytesRead += numBytesRead;
        return FillTupleBufferResult::withBytes(numBytesRead);
    }
}

std::ostream& UnixSocketSource::toString(std::ostream& str) const
{
    return str << std::format("UnixSocketSource(path: {}, totalNumBytesRead: {})", path, totalNumBytesRead.load());
}

}
