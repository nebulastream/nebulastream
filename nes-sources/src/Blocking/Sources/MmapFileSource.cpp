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

#include <Blocking/Sources/MmapFileSource.hpp>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
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

MmapFileSource::MmapFileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersMmap::FILEPATH))
{
}

void MmapFileSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    const auto realPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    if (not realPath)
    {
        throw InvalidConfigParameter("MmapFileSource: could not resolve path: {} - {}", this->filePath, getErrorMessageFromERRNO());
    }
    this->fileDescriptor = ::open(realPath.get(), O_RDONLY);
    if (this->fileDescriptor < 0)
    {
        throw InvalidConfigParameter("MmapFileSource: could not open file: {} - {}", this->filePath, getErrorMessageFromERRNO());
    }
    struct stat fileStat = {};
    if (::fstat(this->fileDescriptor, &fileStat) != 0)
    {
        throw InvalidConfigParameter("MmapFileSource: could not stat file: {} - {}", this->filePath, getErrorMessageFromERRNO());
    }
    this->mapSize = static_cast<size_t>(fileStat.st_size);
    this->cursor = 0;
    if (this->mapSize > 0)
    {
        this->mapBase = ::mmap(nullptr, this->mapSize, PROT_READ, MAP_PRIVATE, this->fileDescriptor, 0);
        if (this->mapBase == MAP_FAILED)
        {
            this->mapBase = nullptr;
            throw InvalidConfigParameter("MmapFileSource: mmap failed: {} - {}", this->filePath, getErrorMessageFromERRNO());
        }
        /// Hint the kernel: sequential scan, and pull the pages in eagerly (cheap when page-cache-resident).
        ::madvise(this->mapBase, this->mapSize, MADV_SEQUENTIAL);
        ::madvise(this->mapBase, this->mapSize, MADV_WILLNEED);
        /// Diagnostic A/B: NES_MMAP_POPULATE=1 bulk-prefaults the whole mapping (one range walk) instead of
        /// paying ~1.6M on-demand minor #PF traps during the per-buffer memcpy. madvise so it's a no-op on
        /// kernels < 5.14.
        if (const char* const pop = std::getenv("NES_MMAP_POPULATE"); pop != nullptr && std::string_view(pop) != "0")
        {
#ifdef MADV_POPULATE_READ
            ::madvise(this->mapBase, this->mapSize, MADV_POPULATE_READ);
#endif
        }
    }
}

void MmapFileSource::close()
{
    if (this->mapBase != nullptr)
    {
        ::munmap(this->mapBase, this->mapSize);
        this->mapBase = nullptr;
    }
    if (this->fileDescriptor >= 0)
    {
        ::close(this->fileDescriptor);
        this->fileDescriptor = -1;
    }
}

BlockingSource::FillTupleBufferResult MmapFileSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&, const size_t offset)
{
    const size_t capacity = tupleBuffer.getBufferSize() - offset;
    const size_t remaining = this->mapSize - this->cursor;
    if (remaining == 0)
    {
        return BlockingSource::FillTupleBufferResult::eos();
    }
    const size_t numBytes = std::min(capacity, remaining);
    std::memcpy(
        tupleBuffer.getAvailableMemoryArea<char>().data() + offset, static_cast<const char*>(this->mapBase) + this->cursor, numBytes);
    this->cursor += numBytes;
    this->totalNumBytesRead += numBytes;
    return BlockingSource::FillTupleBufferResult::withBytes(numBytes);
}

DescriptorConfig::Config MmapFileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMmap>(std::move(config), NAME);
}

std::ostream& MmapFileSource::toString(std::ostream& str) const
{
    str << std::format(
        "\nMmapFileSource(filepath: {}, mapSize: {}, totalNumBytesRead: {})",
        this->filePath,
        this->mapSize,
        this->totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType RegisterMmapFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return MmapFileSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMmapFileSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<MmapFileSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterMmapFileInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string(MMAP_FILE_PATH_PARAMETER)))
    {
        throw InvalidConfigParameter("Mock MmapFileSource cannot use given inline data if a 'file_path' is set");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.try_emplace(
        std::string(MMAP_FILE_PATH_PARAMETER), systestAdaptorArguments.testFilePath.string());

    if (std::ofstream testFile(systestAdaptorArguments.testFilePath); testFile.is_open())
    {
        for (const auto& tuple : systestAdaptorArguments.tuples)
        {
            testFile << tuple << "\n";
        }
        testFile.flush();
        return systestAdaptorArguments.physicalSourceConfig;
    }
    throw TestException("Could not open source file \"{}\"", systestAdaptorArguments.testFilePath);
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterMmapFileFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string(MMAP_FILE_PATH_PARAMETER)))
    {
        throw InvalidConfigParameter("The mock file data source cannot be used if the file_path parameter is already set.");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        std::string(MMAP_FILE_PATH_PARAMETER), systestAdaptorArguments.testFilePath.string());

    return systestAdaptorArguments.physicalSourceConfig;
}

}
