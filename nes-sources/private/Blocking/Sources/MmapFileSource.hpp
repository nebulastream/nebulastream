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

#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

class BufferManager;
/// Whole-file mmap + fd owner (defined in the .cpp). munmap/close happen in its destructor, so the mapping
/// outlives the source: each zero-copy buffer holds a shared_ptr to it and the last one released frees it.
struct MmapFileMapping;

static constexpr std::string_view MMAP_FILE_PATH_PARAMETER = "file_path";

/// The file is mmap'd once in open(). Two modes:
///  - ZERO-COPY (default): each buffer directly ALIASES a 128 KiB window of the mapping -- no copy at all. The
///    source pre-fills (preFillsBuffers()==true) and hands wrapped TupleBuffers (BufferManager::wrapExternalMemory)
///    straight to the pipeline; the SIMDCSV indexer only string_views the raw bytes (read-only, never over-reads
///    past numBytes -- see SIMDCSVScan.hpp), so PROT_READ pages are safe. This removes the single page-cache->buffer
///    copy that read(2) forces (the ~11 GB/s read wall), so a read-bound query is no longer capped by it.
///  - COPY (NES_MMAP_COPY=1): each fillTupleBuffer() memcpys from the mapping into a pool buffer -- one userspace
///    copy, replacing read(2)'s kernel copy. Kept for A/B against the zero-copy path.
/// Setup is done in open() with the engine's buffer provider, NOT in the ctor. Selected via `TYPE MmapFile`.
class MmapFileSource final : public BlockingSource
{
public:
    static constexpr std::string_view NAME = "MmapFile";

    explicit MmapFileSource(const SourceDescriptor& sourceDescriptor);
    ~MmapFileSource() override = default;

    MmapFileSource(const MmapFileSource&) = delete;
    MmapFileSource& operator=(const MmapFileSource&) = delete;
    MmapFileSource(MmapFileSource&&) = delete;
    MmapFileSource& operator=(MmapFileSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken, size_t offset) override;

    /// Zero-copy path: the runner drains takePreFilledBuffer() instead of allocating a pool buffer + fillTupleBuffer.
    [[nodiscard]] bool preFillsBuffers() const override { return zeroCopy; }

    [[nodiscard]] std::optional<TupleBuffer> takePreFilledBuffer(const std::stop_token& stopToken) override;

    /// mmap the file.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// munmap + close fd.
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string filePath;
    /// COPY-mode state (NES_MMAP_COPY=1):
    int fileDescriptor = -1;
    void* mapBase = nullptr; ///< base of the whole-file mapping (page-aligned), or nullptr for an empty file
    size_t mapSize = 0; ///< file size in bytes = mapping length
    size_t cursor = 0; ///< next byte to hand out (both modes)
    std::atomic<size_t> totalNumBytesRead = 0;
    /// ZERO-COPY-mode state (default):
    bool zeroCopy = true; ///< NES_MMAP_COPY=1 restores the userspace-memcpy path
    std::shared_ptr<MmapFileMapping> mapping; ///< whole-file mapping, kept alive by every in-flight wrapped buffer
    std::shared_ptr<BufferManager> wrapPool; ///< the private source pool, used to mint wrapped (aliasing) buffers
    size_t windowSize = 0; ///< bytes handed out per buffer (= pool/operator buffer size)
    /// NES_FILE_REPEAT=N steady-window replay (mirrors BlockingFileSource): on EOS re-scan the map from byte 0.
    std::size_t numPasses = 1;
    std::size_t passesDone = 0;
};

struct ConfigParametersMmap
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string(MMAP_FILE_PATH_PARAMETER),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

}
