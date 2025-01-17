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
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <Configurations/Descriptor.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES::Sources
{

namespace asio = boost::asio;

FileSource::FileSource(const SourceDescriptor& sourceDescriptor) : filePath(sourceDescriptor.getFromConfig(ConfigParametersFile::FILEPATH))
{
}

asio::awaitable<void> FileSource::open(asio::io_context& ioc)
{
    fileDescriptor.emplace(::open(filePath.c_str(), O_RDONLY));

    NES_DEBUG("FileSource: Opened file: {}", filePath);
    NES_DEBUG("FileSource: File descriptor: {}", fileDescriptor.value());

    if (fileDescriptor == -1)
    {
        throw CannotOpenSource("FileSource: Failed to open file: {}", filePath);
    }

    fileStream.emplace(asio::posix::stream_descriptor{ioc, fileDescriptor.value()});
    co_return;
}

asio::awaitable<AsyncSource::InternalSourceResult> FileSource::fillBuffer(IOBuffer& buffer)
{
    INVARIANT(fileStream.has_value() && fileStream->is_open(), "FileSource::fillBuffer: File is not open.");

    auto [errorCode, bytesRead] = co_await asio::async_read(
        fileStream.value(), asio::mutable_buffer(buffer.getBuffer(), buffer.getBufferSize()), asio::as_tuple(asio::use_awaitable));

    if (errorCode)
    {
        if (errorCode == asio::error::eof)
        {
            co_return EndOfStream{.bytesRead = bytesRead};
        }
        if (errorCode == asio::error::operation_aborted)
        {
            co_return Cancelled{.bytesRead = bytesRead};
        }
        co_return Error{boost::system::system_error{errorCode}};
    }
    co_return Continue{};
}

void FileSource::cancel()
{
    fileStream->cancel();
}

void FileSource::close()
{
    try
    {
        fileStream->close();
    }
    catch (const boost::system::system_error& e)
    {
        NES_DEBUG("FileSource: Failed to close file: {} with error: {}", filePath, e.what());
    }
}

std::unique_ptr<Configurations::DescriptorConfig::Config> FileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersFile>(std::move(config), NAME);
}

std::ostream& FileSource::toString(std::ostream& str) const
{
    str << std::format("\nFileSource(filepath: {})", filePath);
    return str;
}

SourceValidationRegistryReturnType
SourceValidationGeneratedRegistrar::RegisterFileSourceValidation(const SourceValidationRegistryArguments& arguments)
{
    return FileSource::validateAndFormat(arguments.config);
}


SourceRegistryReturnType SourceGeneratedRegistrar::RegisterFileSource(const SourceRegistryArguments& arguments)
{
    return std::make_unique<FileSource>(arguments.sourceDescriptor);
}

}
