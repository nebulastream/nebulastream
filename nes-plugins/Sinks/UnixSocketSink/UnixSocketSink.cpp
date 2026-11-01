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

#include <UnixSocketSink.hpp>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <expected>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <SinksParsing/SchemaFormatter.hpp>
#include <Util/Files.hpp>
#include <Util/Variant.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

UnixSocketSink::Connection::Connection(const std::string& path)
{
    if (path.empty() || path.size() >= sizeof(sockaddr_un{}.sun_path) || path.contains('\0'))
    {
        throw CannotOpenSink("Invalid Unix socket path: {}", path);
    }
    descriptor = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (descriptor < 0)
    {
        const auto errorMessage = getErrorMessageFromERRNO();
        throw CannotOpenSink("Could not create Unix socket for sink: path={}, error={}", path, errorMessage);
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(std::data(address.sun_path), path.c_str(), path.size() + 1);
    /// POSIX connect accepts sockaddr for all address families.
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    if (::connect(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0)
    {
        const auto errorMessage = getErrorMessageFromERRNO();
        ::close(descriptor);
        throw CannotOpenSink("Could not connect to Unix socket sink: path={}, error={}", path, errorMessage);
    }
}

UnixSocketSink::Connection::~Connection()
{
    ::close(descriptor);
}

namespace
{

/// NOLINTBEGIN(bugprone-throwing-static-initialization,cert-err58-cpp)
const ConfigField<std::string> PATH{Identifier::parse("PATH"), "The path to an existing Unix-domain stream socket."};
/// NOLINTEND(bugprone-throwing-static-initialization,cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> UnixSocketSink::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("UNIX_SOCKET_SINK"), PATH);
}

std::expected<UnixSocketSinkConfig, Exception> UnixSocketSinkConfig::fromConfig(const InstantiatedConfig& config)
{
    return UnixSocketSinkConfig{.path = config.get(PATH)};
}

UnixSocketSink::UnixSocketSink(
    BackpressureController backpressureController, const UnixSocketSinkConfig& config, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , path(config.path)
    , schemaFormatter(NES::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptor.getSchema()))
{
}

std::ostream& UnixSocketSink::toString(std::ostream& str) const
{
    return str << "UnixSocketSink(path: " << path << ")";
}

void UnixSocketSink::start(PipelineExecutionContext&)
{
    const auto destination = connection.wlock();
    destination->reset();
    destination->emplace(path);
    try
    {
        write(destination->value(), schemaFormatter.getFormattedSchema());
    }
    catch (...)
    {
        destination->reset();
        throw;
    }
}

void UnixSocketSink::write(const Connection& destination, std::string_view contents) const
{
    while (!contents.empty())
    {
        const auto count = ::send(destination.descriptor, contents.data(), contents.size(), MSG_NOSIGNAL);
        if (count < 0 && errno == EINTR)
        {
            continue;
        }
        if (count <= 0)
        {
            const auto errorMessage = getErrorMessage(count == 0 ? EIO : errno);
            throw CannotOpenSink("Could not write to Unix socket sink: path={}, error={}", path, errorMessage);
        }
        contents.remove_prefix(static_cast<size_t>(count));
    }
}

void UnixSocketSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in UnixSocketSink.");
    const auto destination = connection.wlock();
    PRECONDITION(destination->has_value(), "Unix socket sink was not opened");

    BufferIterator iterator{inputTupleBuffer};
    while (const auto element = iterator.getNextElement())
    {
        write(destination->value(), {element->buffer.getAvailableMemoryArea<char>().data(), element->contentLength});
    }
}

void UnixSocketSink::stop(PipelineExecutionContext&)
{
    connection.wlock()->reset();
}

}
