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
#include <filesystem>
#include <format>
#include <fstream>
#include <ios>
#include <istream>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Files.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

static BenchmarkMode parseBenchmarkMode(const std::string& modeStr)
{
    if (modeStr == "file")
    {
        return BenchmarkMode::File;
    }
    if (modeStr == "tmpfs_cold")
    {
        return BenchmarkMode::TmpfsCold;
    }
    if (modeStr == "tmpfs_warm")
    {
        return BenchmarkMode::TmpfsWarm;
    }
    if (modeStr == "memory")
    {
        return BenchmarkMode::Memory;
    }
    if (modeStr == "in_place")
    {
        return BenchmarkMode::InPlace;
    }
    throw InvalidConfigParameter("Invalid benchmark mode '{}'. Expected one of: file, tmpfs_cold, tmpfs_warm, memory, in_place", modeStr);
}

BenchmarkSource::BenchmarkSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersBenchmark::FILEPATH))
    , mode(parseBenchmarkMode(sourceDescriptor.getFromConfig(ConfigParametersBenchmark::MODE)))
    , tmpfsPath(sourceDescriptor.getFromConfig(ConfigParametersBenchmark::TMPFS_PATH))
    , loop(sourceDescriptor.getFromConfig(ConfigParametersBenchmark::LOOP))
{
}

BenchmarkSource::~BenchmarkSource()
{
    if (fileData)
    {
        std::free(fileData);
        fileData = nullptr;
    }
}

void BenchmarkSource::open(std::shared_ptr<AbstractBufferProvider> /*bufferProvider*/)
{
    const auto realPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    if (!realPath)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath, getErrorMessageFromERRNO());
    }

    switch (mode)
    {
        case BenchmarkMode::File:
        {
            inputFile = std::ifstream(realPath.get(), std::ios::binary);
            if (!inputFile)
            {
                throw InvalidConfigParameter("Could not open file: {}", filePath);
            }
            break;
        }

        case BenchmarkMode::Memory:
        {
            std::ifstream file(realPath.get(), std::ios::binary | std::ios::ate);
            if (!file)
            {
                throw InvalidConfigParameter("Could not open file: {}", filePath);
            }
            const auto fileSize = static_cast<size_t>(file.tellg());
            file.seekg(0, std::ios::beg);
            memoryBuffer.resize(fileSize);
            file.read(memoryBuffer.data(), static_cast<std::streamsize>(fileSize));
            memoryReadOffset = 0;
            break;
        }

        case BenchmarkMode::TmpfsCold:
        case BenchmarkMode::TmpfsWarm:
        {
            namespace fs = std::filesystem;
            if (!fs::is_directory(tmpfsPath))
            {
                throw InvalidConfigParameter("tmpfs_path '{}' is not a valid directory", tmpfsPath);
            }
            const fs::path srcPath(realPath.get());
            const fs::path destDir(tmpfsPath);
            tmpfsCopyPath = (destDir / ("nes_benchmark_" + std::to_string(getpid()) + "_" + srcPath.filename().string())).string();
            fs::copy_file(srcPath, tmpfsCopyPath, fs::copy_options::overwrite_existing);

            if (mode == BenchmarkMode::TmpfsWarm)
            {
                /// Read the entire file once to warm the page cache, then rewind.
                std::ifstream warmFile(tmpfsCopyPath, std::ios::binary);
                if (!warmFile)
                {
                    throw InvalidConfigParameter("Could not open tmpfs copy for warming: {}", tmpfsCopyPath);
                }
                std::vector<char> discardBuffer(64 * 1024);
                while (warmFile.read(discardBuffer.data(), static_cast<std::streamsize>(discardBuffer.size())) || warmFile.gcount() > 0)
                {
                    /// intentionally discard data — just touching pages
                }
            }

            inputFile = std::ifstream(tmpfsCopyPath, std::ios::binary);
            if (!inputFile)
            {
                throw InvalidConfigParameter("Could not open tmpfs copy: {}", tmpfsCopyPath);
            }
            break;
        }

        case BenchmarkMode::InPlace:
        {
            std::ifstream file(realPath.get(), std::ios::binary | std::ios::ate);
            if (!file)
            {
                throw InvalidConfigParameter("Could not open file: {}", filePath);
            }
            fileDataSize = static_cast<size_t>(file.tellg());
            file.seekg(0, std::ios::beg);

            constexpr size_t ALIGNMENT = 64;

            /// Allocate aligned memory for the entire file
            const auto alignedSize = fileDataSize > 0 ? fileDataSize + (ALIGNMENT - fileDataSize % ALIGNMENT) % ALIGNMENT : ALIGNMENT;
            fileData = static_cast<uint8_t*>(std::aligned_alloc(ALIGNMENT, alignedSize));
            if (!fileData)
            {
                throw InvalidConfigParameter("Failed to allocate {} bytes for in-place benchmark source", alignedSize);
            }
            file.read(reinterpret_cast<char*>(fileData), static_cast<std::streamsize>(fileDataSize));
            memoryReadOffset = 0;
            break;
        }
    }
}

void BenchmarkSource::close()
{
    inputFile.close();
    memoryBuffer.clear();
    memoryBuffer.shrink_to_fit();
    memoryReadOffset = 0;

    memoryReadOffset = 0;

    if (fileData)
    {
        std::free(fileData);
        fileData = nullptr;
        fileDataSize = 0;
    }

    /// Clean up tmpfs copy
    if ((mode == BenchmarkMode::TmpfsCold || mode == BenchmarkMode::TmpfsWarm) && !tmpfsCopyPath.empty())
    {
        std::filesystem::remove(tmpfsCopyPath);
        tmpfsCopyPath.clear();
    }
}

Source::FillTupleBufferResult BenchmarkSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    FillTupleBufferResult result = FillTupleBufferResult::eos();

    switch (mode)
    {
        case BenchmarkMode::File:
        case BenchmarkMode::TmpfsCold:
        case BenchmarkMode::TmpfsWarm:
            result = fillFromFile(tupleBuffer);
            break;
        case BenchmarkMode::Memory:
            result = fillFromMemory(tupleBuffer);
            break;
        case BenchmarkMode::InPlace:
            result = fillFromInPlace(tupleBuffer);
            break;
    }

    if (result.isEoS() && loop)
    {
        rewindSource();
        switch (mode)
        {
            case BenchmarkMode::File:
            case BenchmarkMode::TmpfsCold:
            case BenchmarkMode::TmpfsWarm:
                result = fillFromFile(tupleBuffer);
                break;
            case BenchmarkMode::Memory:
                result = fillFromMemory(tupleBuffer);
                break;
            case BenchmarkMode::InPlace:
                result = fillFromInPlace(tupleBuffer);
                break;
        }
    }

    if (!result.isEoS())
    {
        totalNumBytesRead += result.getNumberOfBytes();
    }

    return result;
}

Source::FillTupleBufferResult BenchmarkSource::fillFromFile(TupleBuffer& tupleBuffer)
{
    inputFile.read(tupleBuffer.getAvailableMemoryArea<std::istream::char_type>().data(),
                   static_cast<std::streamsize>(tupleBuffer.getBufferSize()));
    const auto numBytesRead = inputFile.gcount();
    if (numBytesRead == 0)
    {
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(numBytesRead);
}

Source::FillTupleBufferResult BenchmarkSource::fillFromMemory(TupleBuffer& tupleBuffer)
{
    if (memoryReadOffset >= memoryBuffer.size())
    {
        return FillTupleBufferResult::eos();
    }

    const auto bytesToCopy = std::min(static_cast<size_t>(tupleBuffer.getBufferSize()), memoryBuffer.size() - memoryReadOffset);
    if (bytesToCopy == 0)
    {
        return FillTupleBufferResult::eos();
    }

    std::memcpy(tupleBuffer.getAvailableMemoryArea<char>().data(), memoryBuffer.data() + memoryReadOffset, bytesToCopy);
    memoryReadOffset += bytesToCopy;
    return FillTupleBufferResult::withBytes(bytesToCopy);
}

Source::FillTupleBufferResult BenchmarkSource::fillFromInPlace(TupleBuffer& tupleBuffer)
{
    if (memoryReadOffset >= fileDataSize)
    {
        return FillTupleBufferResult::eos();
    }

    const auto bytesToCopy = std::min(static_cast<size_t>(tupleBuffer.getBufferSize()), fileDataSize - memoryReadOffset);
    if (bytesToCopy == 0)
    {
        return FillTupleBufferResult::eos();
    }

    std::memcpy(tupleBuffer.getAvailableMemoryArea<char>().data(), fileData + memoryReadOffset, bytesToCopy);
    memoryReadOffset += bytesToCopy;
    return FillTupleBufferResult::withBytes(bytesToCopy);
}

void BenchmarkSource::rewindSource()
{
    switch (mode)
    {
        case BenchmarkMode::File:
        case BenchmarkMode::TmpfsCold:
        case BenchmarkMode::TmpfsWarm:
            inputFile.clear();
            inputFile.seekg(0, std::ios::beg);
            break;
        case BenchmarkMode::Memory:
            memoryReadOffset = 0;
            break;
        case BenchmarkMode::InPlace:
            memoryReadOffset = 0;
            break;
    }
}

DescriptorConfig::Config BenchmarkSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersBenchmark>(std::move(config), NAME);
}

std::ostream& BenchmarkSource::toString(std::ostream& str) const
{
    str << std::format(
        "\nBenchmarkSource(filepath: {}, mode: {}, loop: {}, totalNumBytesRead: {})",
        this->filePath,
        static_cast<int>(this->mode),
        this->loop,
        this->totalNumBytesRead.load());
    return str;
}

/// ---- Registry registration functions ----

SourceValidationRegistryReturnType RegisterBenchmarkSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return BenchmarkSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterBenchmarkSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<BenchmarkSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterBenchmarkInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains("file_path"))
    {
        throw InvalidConfigParameter("Mock BenchmarkSource cannot use given inline data if a 'file_path' is set");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.try_emplace("file_path", systestAdaptorArguments.testFilePath.string());

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

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterBenchmarkFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains("file_path"))
    {
        throw InvalidConfigParameter("The mock file data source cannot be used if the file_path parameter is already set.");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace("file_path", systestAdaptorArguments.testFilePath.string());
    return systestAdaptorArguments.physicalSourceConfig;
}

}
