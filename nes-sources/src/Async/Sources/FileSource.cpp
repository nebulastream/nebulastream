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

#include <Async/Sources/FileSource.hpp>

#include <fcntl.h>

#include <cerrno>
#include <cstring>
#include <format>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/system/system_error.hpp>

#include <Configurations/Descriptor.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES::Sources
{

FileSource::FileSource(const SourceDescriptor& sourceDescriptor) : filePath(sourceDescriptor.getFromConfig(ConfigParametersFile::FILEPATH))
{
}

asio::awaitable<void, Executor> FileSource::open()
{
    PRECONDITION(
        !fileDescriptor.has_value() && !fileStream.has_value(), "open() may not be called twice on the same instance.");
    fileDescriptor.emplace(::open(filePath.c_str(), O_RDONLY));

    if (fileDescriptor == -1)
    {
        throw CannotOpenSource("Failed to open file: {}", filePath);
    }

    fileStream.emplace(asio::posix::stream_descriptor{co_await asio::this_coro::executor, fileDescriptor.value()});
    NES_DEBUG("Opened file: {}", filePath);
}

asio::awaitable<AsyncSource::InternalSourceResult, Executor> FileSource::fillBuffer(IOBuffer& buffer)
{
    PRECONDITION(fileStream.has_value() && fileStream->is_open(), "File is not open.");
    if ((co_await asio::this_coro::cancellation_state).cancelled() == asio::cancellation_type::terminal)
    {
        co_return Cancelled{};
    }

    auto [errorCode, bytesRead] = co_await async_read(
        fileStream.value(), asio::mutable_buffer(buffer.getBuffer(), buffer.getBufferSize()), as_tuple(asio::deferred));

    if (errorCode)
    {
        if (errorCode == asio::error::eof)
        {
            co_return EndOfStream{.bytesRead = bytesRead};
        }
        if (errorCode == asio::error::operation_aborted)
        {
            co_return Cancelled{};
        }
        const auto message = std::format("Failed to read from file {} with errorCode: '{}'", filePath, errorCode.message());
        co_return Error{.exception = DataIngestionFailure(message)};
    }
    co_return Continue{.bytesRead = bytesRead};
}

void FileSource::close()
{
    if (fileStream->is_open())
    {
        fileStream->close();
        NES_DEBUG("Closed file stream.");
    }
}

std::ostream& FileSource::toString(std::ostream& str) const
{
    return str << std::format("\nFileSource(filepath: {})", filePath);
}

Configurations::DescriptorConfig::Config FileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersFile>(std::move(config), NAME);
}

SourceValidationRegistryReturnType SourceValidationGeneratedRegistrar::RegisterFileSourceValidation(SourceValidationRegistryArguments arguments)
{
    return FileSource::validateAndFormat(arguments.config);
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterFileSource(SourceRegistryArguments arguments)
{
    return std::make_unique<FileSource>(arguments.sourceDescriptor);
}

}
