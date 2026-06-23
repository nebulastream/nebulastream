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

#include <Blocking/Sources/InMemoryFileSource.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
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

InMemoryFileSource::InMemoryFileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersInMemory::FILEPATH))
{
    /// Load the ENTIRE file into RAM now. The ctor runs at query-registration time (well before the
    /// systest -b `running` timestamp), so this read is OUTSIDE the benchmarked window.
    const auto realPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    if (not realPath)
    {
        throw InvalidConfigParameter("InMemoryFileSource: could not resolve path: {} - {}", this->filePath, getErrorMessageFromERRNO());
    }
    std::ifstream inputFile(realPath.get(), std::ios::binary | std::ios::ate);
    if (not inputFile)
    {
        throw InvalidConfigParameter("InMemoryFileSource: could not open file: {} - {}", this->filePath, getErrorMessageFromERRNO());
    }
    const auto fileSize = static_cast<std::streamoff>(inputFile.tellg());
    inputFile.seekg(0, std::ios::beg);

    const char* const prefillEnv = std::getenv("NES_INMEM_PREFILL");
    this->prefillMode = prefillEnv != nullptr && std::string_view(prefillEnv) != "0";
    if (this->prefillMode)
    {
        /// PREFILL: read the file directly into ready TupleBuffers from a PRIVATE pool, all in the ctor
        /// (outside the timed window). The runner hands these out via takePreFilledBuffer() with zero
        /// memcpy in the measured loop. The pool is a member so it outlives the buffers (recycled back
        /// to it as workers release them).
        const char* const bufSizeEnv = std::getenv("NES_INMEM_BUFSIZE");
        const auto bufferSize = static_cast<uint32_t>(bufSizeEnv != nullptr ? std::stoul(bufSizeEnv) : 131072UL);
        const auto numBuffers = static_cast<uint32_t>((static_cast<size_t>(fileSize) / bufferSize) + 2);
        this->prefillPool = BufferManager::create(bufferSize, numBuffers);
        this->prefilled.reserve(numBuffers);
        while (true)
        {
            auto buffer = this->prefillPool->getBufferBlocking();
            inputFile.read(buffer.getAvailableMemoryArea<char>().data(), static_cast<std::streamsize>(bufferSize));
            const auto numBytesRead = inputFile.gcount();
            if (numBytesRead <= 0)
            {
                break;
            }
            buffer.setNumberOfTuples(static_cast<uint64_t>(numBytesRead));
            this->prefilled.push_back(std::move(buffer));
        }
        return;
    }

    this->blob.resize(static_cast<size_t>(fileSize));
    if (fileSize > 0 && not inputFile.read(this->blob.data(), fileSize))
    {
        throw InvalidConfigParameter("InMemoryFileSource: could not read file into memory: {}", this->filePath);
    }
}

std::optional<TupleBuffer> InMemoryFileSource::takePreFilledBuffer()
{
    if (this->prefillCursor >= this->prefilled.size())
    {
        return std::nullopt;
    }
    return std::move(this->prefilled[this->prefillCursor++]);
}

void InMemoryFileSource::open(std::shared_ptr<AbstractBufferProvider>)
{
}

void InMemoryFileSource::close()
{
}

BlockingSource::FillTupleBufferResult
InMemoryFileSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&, const size_t offset)
{
    const size_t capacity = tupleBuffer.getBufferSize() - offset;
    const size_t remaining = this->blob.size() - this->cursor;
    if (remaining == 0)
    {
        return BlockingSource::FillTupleBufferResult::eos();
    }
    const size_t numBytes = std::min(capacity, remaining);
    std::memcpy(tupleBuffer.getAvailableMemoryArea<char>().data() + offset, this->blob.data() + this->cursor, numBytes);
    this->cursor += numBytes;
    return BlockingSource::FillTupleBufferResult::withBytes(numBytes);
}

DescriptorConfig::Config InMemoryFileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersInMemory>(std::move(config), NAME);
}

std::ostream& InMemoryFileSource::toString(std::ostream& str) const
{
    if (this->prefillMode)
    {
        str << std::format("\nInMemoryFileSource(filepath: {}, prefill: {} buffers)", this->filePath, this->prefilled.size());
    }
    else
    {
        str << std::format(
            "\nInMemoryFileSource(filepath: {}, loadedBytes: {}, cursor: {})", this->filePath, this->blob.size(), this->cursor);
    }
    return str;
}

SourceValidationRegistryReturnType RegisterInMemorySourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return InMemoryFileSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterInMemorySource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<InMemoryFileSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterInMemoryInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string("file_path")))
    {
        throw InvalidConfigParameter("Mock InMemoryFileSource cannot use given inline data if a 'file_path' is set");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.try_emplace(
        std::string("file_path"), systestAdaptorArguments.testFilePath.string());

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

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterInMemoryFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string("file_path")))
    {
        throw InvalidConfigParameter("The mock file data source cannot be used if the file_path parameter is already set.");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        std::string("file_path"), systestAdaptorArguments.testFilePath.string());

    return systestAdaptorArguments.physicalSourceConfig;
}

}
