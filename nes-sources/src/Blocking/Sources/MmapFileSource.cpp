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
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
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
#include <Sources/SourceUtility.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Files.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

/// Whole-file mapping owner. munmap + close run in the destructor, so the mapping stays valid until the LAST
/// holder drops its shared_ptr: the source holds one, and every in-flight zero-copy buffer holds one (captured
/// in its release hook). This is what makes handing raw mmap windows to the pipeline safe.
struct MmapFileMapping
{
    int fileDescriptor = -1;
    void* base = nullptr;
    size_t size = 0;
    MmapFileMapping() = default;
    MmapFileMapping(const MmapFileMapping&) = delete;
    MmapFileMapping& operator=(const MmapFileMapping&) = delete;
    MmapFileMapping(MmapFileMapping&&) = delete;
    MmapFileMapping& operator=(MmapFileMapping&&) = delete;

    ~MmapFileMapping()
    {
        if (base != nullptr && base != MAP_FAILED)
        {
            ::munmap(base, size);
        }
        if (fileDescriptor >= 0)
        {
            ::close(fileDescriptor);
        }
    }
};

MmapFileSource::MmapFileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersMmap::FILEPATH))
{
    /// Default = zero-copy (alias mmap windows). NES_MMAP_COPY=1 restores the userspace-memcpy-into-pool path.
    if (const char* const env = std::getenv("NES_MMAP_COPY"); env != nullptr && std::string_view(env) != "0")
    {
        this->zeroCopy = false;
    }
    /// NES_FILE_REPEAT=N (zero-copy path only): re-scan the map N times for a long steady benchmark window.
    if (const char* const env = std::getenv("NES_FILE_REPEAT"))
    {
        this->numPasses = std::max<std::size_t>(1, std::strtoul(env, nullptr, 10));
    }
}

void MmapFileSource::open(std::shared_ptr<AbstractBufferProvider> bufferProvider)
{
    const auto realPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    if (not realPath)
    {
        throw InvalidConfigParameter("MmapFileSource: could not resolve path: {} - {}", this->filePath, getErrorMessageFromERRNO());
    }
    const int fd = ::open(realPath.get(), O_RDONLY);
    if (fd < 0)
    {
        throw InvalidConfigParameter("MmapFileSource: could not open file: {} - {}", this->filePath, getErrorMessageFromERRNO());
    }
    struct stat fileStat = {};
    if (::fstat(fd, &fileStat) != 0)
    {
        ::close(fd);
        throw InvalidConfigParameter("MmapFileSource: could not stat file: {} - {}", this->filePath, getErrorMessageFromERRNO());
    }
    const auto fileSize = static_cast<size_t>(fileStat.st_size);
    this->cursor = 0;
    void* base = nullptr;
    if (fileSize > 0)
    {
        base = ::mmap(nullptr, fileSize, PROT_READ, MAP_PRIVATE, fd, 0);
        if (base == MAP_FAILED)
        {
            ::close(fd);
            throw InvalidConfigParameter("MmapFileSource: mmap failed: {} - {}", this->filePath, getErrorMessageFromERRNO());
        }
        /// Hint the kernel: sequential scan, and pull the pages in eagerly (cheap when page-cache-resident).
        ::madvise(base, fileSize, MADV_SEQUENTIAL);
        ::madvise(base, fileSize, MADV_WILLNEED);
        /// Diagnostic A/B: NES_MMAP_POPULATE=1 bulk-prefaults the whole mapping (one range walk) instead of
        /// paying on-demand minor #PF traps on first touch. madvise so it's a no-op on kernels < 5.14.
        if (const char* const pop = std::getenv("NES_MMAP_POPULATE"); pop != nullptr && std::string_view(pop) != "0")
        {
#ifdef MADV_POPULATE_READ
            ::madvise(base, fileSize, MADV_POPULATE_READ);
#endif
        }
    }

    if (this->zeroCopy)
    {
        /// The private source pool is a BufferManager (SourceProvider::lower). We use it purely as the factory
        /// for wrapped (aliasing) buffers -- no pooled memory is consumed on this path.
        this->wrapPool = std::dynamic_pointer_cast<BufferManager>(bufferProvider);
        if (this->wrapPool == nullptr)
        {
            if (base != nullptr)
            {
                ::munmap(base, fileSize);
            }
            ::close(fd);
            throw InvalidConfigParameter("MmapFileSource: zero-copy needs a BufferManager-backed pool");
        }
        this->windowSize = this->wrapPool->getBufferSize();
        auto owned = std::make_shared<MmapFileMapping>();
        owned->fileDescriptor = fd;
        owned->base = base;
        owned->size = fileSize;
        this->mapping = std::move(owned);
        NES_DEBUG("MmapFileSource: zero-copy mapping of {} ({} bytes), window={} B", this->filePath, fileSize, this->windowSize);
    }
    else
    {
        /// COPY mode keeps its own fd + map fields (fillTupleBuffer memcpys out of them).
        this->fileDescriptor = fd;
        this->mapBase = (base == MAP_FAILED) ? nullptr : base;
        this->mapSize = fileSize;
    }
}

void MmapFileSource::close()
{
    /// Zero-copy: drop the source's mapping reference. In-flight buffers keep their own references, so the actual
    /// munmap/close happens when the last wrapped buffer is released (or the pool retaining the segments is torn
    /// down), never while a worker might still be reading the pages.
    this->mapping.reset();
    this->wrapPool.reset();
    /// Copy mode:
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

std::optional<TupleBuffer> MmapFileSource::takePreFilledBuffer(const std::stop_token&)
{
    if (this->mapping == nullptr || this->mapping->base == nullptr)
    {
        return std::nullopt; /// empty file
    }
    const size_t total = this->mapping->size;
    if (this->cursor >= total)
    {
        if (this->passesDone + 1 < this->numPasses)
        {
            ++this->passesDone; /// NES_FILE_REPEAT: rewind the map and stream it again for a steady window
            this->cursor = 0;
        }
        else
        {
            return std::nullopt; /// end of stream
        }
    }
    const size_t len = std::min(this->windowSize, total - this->cursor);
    auto* const windowPtr = static_cast<std::uint8_t*>(this->mapping->base) + this->cursor;
    /// Alias the window: no copy. The release hook holds a shared_ptr to the mapping so the pages outlive this
    /// buffer no matter how long a worker holds it (spanning-tuple stitch, backpressure, etc.).
    TupleBuffer buffer = this->wrapPool->wrapExternalMemory(
        windowPtr, static_cast<std::uint32_t>(len), [keepAlive = this->mapping]() { (void)keepAlive; });
    /// The source hands the formatter raw bytes; it reports the BYTE count as numberOfTuples (the input formatter
    /// derives the tuple count) and stamps read-start so the sink's e2e latency/throughput include ingest.
    buffer.setNumberOfTuples(len);
    buffer.setSourceCreationTimestampInMS(Timestamp(ingestNowMicros()));
    this->cursor += len;
    this->totalNumBytesRead += len;
    return buffer;
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
        "\nMmapFileSource(filepath: {}, mode: {}, mapSize: {}, totalNumBytesRead: {})",
        this->filePath,
        this->zeroCopy ? "zero-copy" : "copy",
        this->zeroCopy ? (this->mapping != nullptr ? this->mapping->size : 0) : this->mapSize,
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
