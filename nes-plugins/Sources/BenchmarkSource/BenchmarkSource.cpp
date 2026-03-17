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

#include <BenchmarkSource.hpp>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <ios>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

BenchmarkSource::BenchmarkSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersBenchmark::FILEPATH))
    , mode(sourceDescriptor.getFromConfig(ConfigParametersBenchmark::MODE))
{
}

BenchmarkSource::~BenchmarkSource()
{
    close();
}

void BenchmarkSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    const auto realPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    if (!realPath)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath, std::strerror(errno));
    }

    if (mode == "mmap")
    {
        mmapFd = ::open(realPath.get(), O_RDONLY);
        if (mmapFd < 0)
        {
            throw InvalidConfigParameter("Could not open file for mmap: {} - {}", realPath.get(), std::strerror(errno));
        }

        struct stat st;
        if (fstat(mmapFd, &st) < 0)
        {
            ::close(mmapFd);
            mmapFd = -1;
            throw InvalidConfigParameter("Could not stat file: {} - {}", realPath.get(), std::strerror(errno));
        }
        fileSize = static_cast<size_t>(st.st_size);

        mmapBase = ::mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, mmapFd, 0);
        if (mmapBase == MAP_FAILED)
        {
            ::close(mmapFd);
            mmapFd = -1;
            mmapBase = nullptr;
            throw InvalidConfigParameter("Could not mmap file: {} - {}", realPath.get(), std::strerror(errno));
        }

        /// Advise the kernel that we will read sequentially
        ::madvise(mmapBase, fileSize, MADV_SEQUENTIAL);
    }
    else
    {
        /// Preload mode: read entire file into memory
        std::ifstream inputFile(realPath.get(), std::ios::binary | std::ios::ate);
        if (!inputFile)
        {
            throw InvalidConfigParameter("Could not open file: {} - {}", realPath.get(), std::strerror(errno));
        }

        fileSize = static_cast<size_t>(inputFile.tellg());
        inputFile.seekg(0, std::ios::beg);

        preloadedData.resize(fileSize);
        inputFile.read(preloadedData.data(), static_cast<std::streamsize>(fileSize));
    }

    readOffset = 0;
}

void BenchmarkSource::close()
{
    if (mmapBase != nullptr && mmapBase != MAP_FAILED)
    {
        ::munmap(mmapBase, fileSize);
        mmapBase = nullptr;
    }
    if (mmapFd >= 0)
    {
        ::close(mmapFd);
        mmapFd = -1;
    }
    preloadedData.clear();
    preloadedData.shrink_to_fit();
    readOffset = 0;
    fileSize = 0;
}

Source::FillTupleBufferResult BenchmarkSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    if (readOffset >= fileSize)
    {
        return FillTupleBufferResult::eos();
    }

    const auto bytesToCopy = std::min(tupleBuffer.getBufferSize(), fileSize - readOffset);

    const char* srcPtr = (mode == "mmap") ? static_cast<const char*>(mmapBase) + readOffset : preloadedData.data() + readOffset;

    std::memcpy(tupleBuffer.getAvailableMemoryArea<char>().data(), srcPtr, bytesToCopy);

    readOffset += bytesToCopy;
    totalNumBytesRead += bytesToCopy;

    return FillTupleBufferResult::withBytes(bytesToCopy);
}

DescriptorConfig::Config BenchmarkSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersBenchmark>(std::move(config), NAME);
}

std::ostream& BenchmarkSource::toString(std::ostream& str) const
{
    str << std::format(
        "\nBenchmarkSource(filepath: {}, mode: {}, fileSize: {}, totalNumBytesRead: {})",
        this->filePath,
        this->mode,
        this->fileSize,
        this->totalNumBytesRead.load());
    return str;
}

/// --- Registry registration functions ---

SourceValidationRegistryReturnType RegisterBenchmarkSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return BenchmarkSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterBenchmarkSource(SourceRegistryArguments args)
{
    return std::make_unique<BenchmarkSource>(args.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterBenchmarkInlineData(InlineDataRegistryArguments args)
{
    if (args.physicalSourceConfig.sourceConfig.contains("file_path"))
    {
        throw InvalidConfigParameter("Mock BenchmarkSource cannot use given inline data if a 'file_path' is set");
    }

    args.physicalSourceConfig.sourceConfig.try_emplace("file_path", args.testFilePath.string());

    if (std::ofstream testFile(args.testFilePath); testFile.is_open())
    {
        for (const auto& tuple : args.tuples)
        {
            testFile << tuple << "\n";
        }
        testFile.flush();
        return args.physicalSourceConfig;
    }
    throw TestException("Could not open source file \"{}\"", args.testFilePath);
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterBenchmarkFileData(FileDataRegistryArguments args)
{
    if (args.physicalSourceConfig.sourceConfig.contains("file_path"))
    {
        throw InvalidConfigParameter("The mock benchmark file data source cannot be used if the file_path parameter is already set.");
    }

    args.physicalSourceConfig.sourceConfig.emplace("file_path", args.testFilePath.string());
    return args.physicalSourceConfig;
}

}
