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

#include "FileDataRegistry.hpp"
#include "InlineDataRegistry.hpp"

namespace NES
{

FileSource::FileSource(const SourceDescriptor& sourceDescriptor) : filePath(sourceDescriptor.getFromConfig(ConfigParametersFileSource::FILEPATH))
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
        fileStream.value(), asio::mutable_buffer(buffer.getMemArea(), buffer.getBufferSize()), asio::as_tuple(asio::deferred));
    buffer.setNumberOfTuples(bytesRead);

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
        co_return Error{.exception = RunningRoutineFailure(message)};
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

DescriptorConfig::Config FileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersFileSource>(std::move(config), NAME);
}

SourceValidationRegistryReturnType SourceValidationGeneratedRegistrar::RegisterFileSourceValidation(SourceValidationRegistryArguments arguments)
{
    return FileSource::validateAndFormat(arguments.config);
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterFileSource(SourceRegistryArguments arguments)
{
    return std::make_unique<FileSource>(arguments.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterFileInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.attachSource.tuples)
    {
        if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(std::string(SYSTEST_FILE_PATH_PARAMETER));
            filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            filePath->second = systestAdaptorArguments.testFilePath;
            if (std::ofstream testFile(systestAdaptorArguments.testFilePath); testFile.is_open())
            {
                /// Write inline tuples to test file.
                for (const auto& tuple : systestAdaptorArguments.attachSource.tuples.value())
                {
                    testFile << tuple << "\n";
                }
                testFile.flush();
                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw TestException("Could not open source file \"{}\"", systestAdaptorArguments.testFilePath);
        }
        throw InvalidConfigParameter("A FileSource config must contain filePath parameter");
    }
    throw TestException("An INLINE SystestAttachSource must not have a 'tuples' vector that is null.");
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterFileFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    /// Check that the test data dir is defined and that the 'filePath' parameter is set
    /// Replace the 'TESTDATA' placeholder in the filepath
    if (const auto attachSourceFilePath = systestAdaptorArguments.attachSource.fileDataPath)
    {
        if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(std::string(SYSTEST_FILE_PATH_PARAMETER));
            filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            filePath->second = attachSourceFilePath.value();
            return systestAdaptorArguments.physicalSourceConfig;
        }
        throw InvalidConfigParameter("A FileSource config must contain filePath parameter.");
    }
    throw InvalidConfigParameter("An attach source of type FileData must contain a filePath configuration.");
}

}
