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

#include <Sinks/FileSink.hpp>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <Util/Files.hpp>
#include <Util/Overloaded.hpp>

#include <Util/Variant.hpp>
#include <fmt/format.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
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
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

FileSink::SocketOutput::SocketOutput(const std::string& path)
{
    if (path.size() >= sizeof(sockaddr_un{}.sun_path) || path.contains('\0'))
    {
        throw CannotOpenSink("Invalid Unix socket path: {}", path);
    }
    descriptor = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (descriptor < 0)
    {
        const auto errorMessage = getErrorMessageFromERRNO();
        throw CannotOpenSink("Could not create Unix socket for sink: filePath={}, error={}", path, errorMessage);
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
        throw CannotOpenSink("Could not connect to Unix socket sink: filePath={}, error={}", path, errorMessage);
    }
}

FileSink::SocketOutput::~SocketOutput()
{
    ::close(descriptor);
}

namespace
{

/// Config fields of the file sink, shared by getConfigSchema (declaration) and
/// FileSinkConfig::fromConfig (typed extraction).
/// NOLINTBEGIN(bugprone-throwing-static-initialization)
const ConfigField<std::filesystem::path> FILE_PATH{
    Identifier::parse("FILE_PATH"),
    "The path to the output file or an existing Unix-domain socket.",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .transform([](const auto& val) { return std::filesystem::path{val}; });
    }};

const ConfigField<bool> APPEND{
    Identifier::parse("APPEND"), "A boolean, where TRUE indicates to append to the specified file, and false to overwrite it.", false};
/// NOLINTEND(bugprone-throwing-static-initialization)

}

Schema<QualifiedErasedConfigField, Ordered> FileSink::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("FILE_SINK"), FILE_PATH, APPEND);
}

std::expected<FileSinkConfig, Exception> FileSinkConfig::fromConfig(const InstantiatedConfig& config)
{
    return FileSinkConfig{.filePath = config.get(FILE_PATH), .append = config.get(APPEND)};
}

FileSink::FileSink(BackpressureController backpressureController, const FileSinkConfig& config, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , outputFilePath(config.filePath)
    , isAppend(config.append)
    , schemaFormatter(
          SchemaFormatter(NES::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptor.getSchema())))
{
}

std::ostream& FileSink::toString(std::ostream& str) const
{
    str << fmt::format("FileSink(filePathOutput: {}, isAppend: {})", outputFilePath, isAppend);
    return str;
}

void FileSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up file sink: {}", *this);
    const auto destination = output.wlock();
    if (std::filesystem::is_socket(outputFilePath))
    {
        destination->emplace<SocketOutput>(outputFilePath);
    }
    else
    {
        /// Remove an existing file unless append mode is requested.
        if (!isAppend && std::filesystem::exists(outputFilePath))
        {
            if (std::error_code errorCode; !std::filesystem::remove(outputFilePath, errorCode))
            {
                throw CannotOpenSink("Could not remove existing output file: filePath={} ", outputFilePath);
            }
        }

        auto& stream = destination->emplace<std::ofstream>();
        stream.open(outputFilePath, std::ofstream::binary | std::ofstream::app);
        if (!stream.is_open() || !stream.good())
        {
            throw CannotOpenSink(
                "Could not open output file; filePathOutput={}, is_open()={}, good={}", outputFilePath, stream.is_open(), stream.good());
        }

        /// Do not repeat the header when appending to a nonempty file.
        if (stream.tellp() != 0)
        {
            return;
        }
    }

    write(*destination, schemaFormatter.getFormattedSchema());
}

void FileSink::write(Output& destination, std::string_view contents)
{
    std::visit(
        Overloaded{
            [](std::monostate) { PRECONDITION(false, "Sink was not opened"); },
            [&](std::ofstream& stream) { stream.write(contents.data(), static_cast<std::streamsize>(contents.size())); },
            [&](SocketOutput& socket)
            {
                while (!contents.empty())
                {
                    const auto count = ::send(socket.descriptor, contents.data(), contents.size(), MSG_NOSIGNAL);
                    if (count < 0 && errno == EINTR)
                    {
                        continue;
                    }
                    if (count <= 0)
                    {
                        const auto errorMessage = getErrorMessage(count == 0 ? EIO : errno);
                        throw CannotOpenSink("Could not write to Unix socket sink: filePath={}, error={}", outputFilePath, errorMessage);
                    }
                    contents.remove_prefix(static_cast<size_t>(count));
                }
            }},
        destination);
}

void FileSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in FileSink.");
    const auto destination = output.wlock();
    PRECONDITION(!std::holds_alternative<std::monostate>(*destination), "Sink was not opened");

    BufferIterator iterator{inputTupleBuffer};
    while (const auto element = iterator.getNextElement())
    {
        write(*destination, {element->buffer.getAvailableMemoryArea<char>().data(), element->contentLength});
    }
    std::visit(Overloaded{[](std::monostate) { }, [](std::ofstream& stream) { stream.flush(); }, [](SocketOutput&) { }}, *destination);
}

void FileSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Closing file sink, filePathOutput={}", outputFilePath);
    const auto destination = output.wlock();
    std::visit(
        Overloaded{
            [](std::monostate) { },
            [](std::ofstream& stream)
            {
                stream.flush();
                stream.close();
            },
            [](SocketOutput&) { }},
        *destination);
    destination->emplace<std::monostate>();
}

}
