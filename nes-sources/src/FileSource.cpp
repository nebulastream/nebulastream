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

#include <FileSource.hpp>

#include <cerrno>
#include <cstring>
#include <expected>
#include <filesystem>
#include <format>
#include <fstream>
#include <ios>
#include <istream>
#include <iterator>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <variant>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/FileSourceConfig.hpp>
#include <Sources/Source.hpp>
#include <Util/Files.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

FileSource::SocketInput::SocketInput(const std::string& path)
{
    if (path.size() >= sizeof(sockaddr_un{}.sun_path) || path.contains('\0'))
    {
        throw CannotOpenSource("Invalid Unix socket path: {}", path);
    }
    descriptor = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (descriptor < 0)
    {
        const auto errorMessage = getErrorMessageFromERRNO();
        throw CannotOpenSource("Could not create Unix socket for source: filePath={}, error={}", path, errorMessage);
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
        throw CannotOpenSource("Could not connect to Unix socket source: filePath={}, error={}", path, errorMessage);
    }
}

FileSource::SocketInput::~SocketInput()
{
    ::close(descriptor);
}

namespace
{

/// NOLINTBEGIN(bugprone-throwing-static-initialization)
const ConfigField<std::filesystem::path> FILE_PATH{
    Identifier::parse("FILE_PATH"),
    "The path to the input file or an existing Unix-domain socket.",
    [](const ConfigLiteral& literal)
    {
        return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
            .transform([](const auto& val) { return std::filesystem::path{val}; });
    }};
/// NOLINTEND(bugprone-throwing-static-initialization)

}

Schema<QualifiedErasedConfigField, Ordered> FileSource::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("FILE_SOURCE"), FILE_PATH);
}

std::expected<FileSourceConfig, Exception> FileSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    return FileSourceConfig{.filePath = config.get(FILE_PATH)};
}

FileSource::FileSource(const FileSourceConfig& config) : filePath(config.filePath)
{
}

void FileSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    if (std::filesystem::is_socket(filePath))
    {
        input.emplace<SocketInput>(filePath);
        return;
    }

    const auto& stream = input.emplace<std::ifstream>(filePath, std::ios::binary);
    if (!stream)
    {
        const auto errorMessage = getErrorMessageFromERRNO();
        throw CannotOpenSource("Could not open input file: filePath={}, error={}", filePath, errorMessage);
    }
}

void FileSource::close()
{
    input.emplace<std::monostate>();
}

Source::FillTupleBufferResult FileSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    const auto memory = tupleBuffer.getAvailableMemoryArea<char>();
    const auto numBytesRead = std::visit(
        Overloaded{
            [](std::monostate) -> size_t { throw CannotReadFromSource("File source was not opened"); },
            [&](std::ifstream& stream)
            {
                stream.read(memory.data(), static_cast<std::streamsize>(tupleBuffer.getBufferSize()));
                return static_cast<size_t>(stream.gcount());
            },
            [&](SocketInput& socket)
            {
                /// Interrupt an idle read on cancellation without closing/reusing its descriptor concurrently.
                const std::stop_callback onStop{stopToken, [&socket] { ::shutdown(socket.descriptor, SHUT_RD); }};
                while (true)
                {
                    const auto count = ::recv(socket.descriptor, memory.data(), tupleBuffer.getBufferSize(), 0);
                    if (count < 0 && errno == EINTR)
                    {
                        continue;
                    }
                    if (count < 0)
                    {
                        const auto errorMessage = getErrorMessageFromERRNO();
                        throw CannotReadFromSource("Could not read from Unix socket source: filePath={}, error={}", filePath, errorMessage);
                    }
                    return static_cast<size_t>(count);
                }
            }},
        input);
    this->totalNumBytesRead += numBytesRead;
    if (numBytesRead == 0)
    {
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(numBytesRead);
}

std::ostream& FileSource::toString(std::ostream& str) const
{
    str << std::format("\nFileSource(filepath: {}, totalNumBytesRead: {})", this->filePath, this->totalNumBytesRead.load());
    return str;
}

}
