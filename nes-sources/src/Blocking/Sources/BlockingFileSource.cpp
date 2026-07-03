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

#include <Blocking/Sources/BlockingFileSource.hpp>

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Files.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

BlockingFileSource::BlockingFileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH))
{
    if (const char* const env = std::getenv("NES_FILE_REPEAT"))
    {
        numPasses = std::max<std::size_t>(1, std::strtoul(env, nullptr, 10));
    }
}

void BlockingFileSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    if (not realCSVPath)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath.c_str(), getErrorMessageFromERRNO());
    }
    this->fileDescriptor = ::open(realCSVPath.get(), O_RDONLY);
    if (this->fileDescriptor == -1)
    {
        throw InvalidConfigParameter("Could not open file: {} - {}", this->filePath.c_str(), getErrorMessageFromERRNO());
    }
}

void BlockingFileSource::close()
{
    if (this->fileDescriptor != -1)
    {
        ::close(this->fileDescriptor);
        this->fileDescriptor = -1;
    }
}

BlockingSource::FillTupleBufferResult
BlockingFileSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&, const size_t offset)
{
    char* const dst = tupleBuffer.getAvailableMemoryArea<char>().data() + offset;
    const std::size_t capacity = tupleBuffer.getBufferSize() - offset;
    /// Read straight into the pool buffer with ::read (no ifstream double-buffering). A single read()
    /// on a regular file may short-return, so loop until the buffer is full or EOF; the formatter then
    /// string_views into this buffer exactly as on the parallel/io_uring paths.
    const auto readFully = [this](char* buf, std::size_t n) -> std::size_t
    {
        std::size_t got = 0;
        while (got < n)
        {
            const ssize_t r = ::read(this->fileDescriptor, buf + got, n - got);
            if (r <= 0) /// 0 = EOF, <0 = error: end this pass (matches the old gcount()==0 behaviour)
            {
                break;
            }
            got += static_cast<std::size_t>(r);
        }
        return got;
    };
    std::size_t numBytesRead = readFully(dst, capacity);
    if (numBytesRead == 0 && this->passesDone + 1 < this->numPasses)
    {
        /// NES_FILE_REPEAT=N: hit EOF but more passes remain -> rewind and re-read into this buffer
        /// so the source keeps streaming a long steady window (mirrors the async FileSource).
        ++this->passesDone;
        ::lseek(this->fileDescriptor, 0, SEEK_SET);
        numBytesRead = readFully(dst, capacity);
    }
    this->totalNumBytesRead += numBytesRead;
    if (numBytesRead == 0)
    {
        return BlockingSource::FillTupleBufferResult::eos();
    }
    return BlockingSource::FillTupleBufferResult::withBytes(numBytesRead);
}

DescriptorConfig::Config BlockingFileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
}

std::ostream& BlockingFileSource::toString(std::ostream& str) const
{
    str << std::format("\nBlockingFileSource(filepath: {}, totalNumBytesRead: {})", this->filePath, this->totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType RegisterBlockingFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return BlockingFileSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterBlockingFileSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<BlockingFileSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType
InlineDataGeneratedRegistrar::RegisterBlockingFileInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string(SYSTEST_FILE_PATH_PARAMETER)))
    {
        throw InvalidConfigParameter("Mock BlockingFileSource cannot use given inline data if a 'file_path' is set");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.try_emplace(
        std::string(SYSTEST_FILE_PATH_PARAMETER), systestAdaptorArguments.testFilePath.string());


    if (std::ofstream testFile(systestAdaptorArguments.testFilePath); testFile.is_open())
    {
        /// Write inline tuples to test file.
        for (const auto& tuple : systestAdaptorArguments.tuples)
        {
            testFile << tuple << "\n";
        }
        testFile.flush();
        return systestAdaptorArguments.physicalSourceConfig;
    }
    throw TestException("Could not open source file \"{}\"", systestAdaptorArguments.testFilePath);
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterBlockingFileFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string(SYSTEST_FILE_PATH_PARAMETER)))
    {
        throw InvalidConfigParameter("The mock file data source cannot be used if the file_path parameter is already set.");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        std::string(SYSTEST_FILE_PATH_PARAMETER), systestAdaptorArguments.testFilePath.string());

    return systestAdaptorArguments.physicalSourceConfig;
}


}
