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

static constexpr std::string_view MMAP_FILE_PATH_PARAMETER = "file_path";

/// Like BlockingFileSource, but the file is mmap'd once in open() and each fillTupleBuffer() does a
/// userspace memcpy from the mapped region into the (pool-owned) target buffer -- instead of an
/// std::ifstream::read() that goes through the read(2) syscall + the kernel `_copy_to_iter` page-cache copy.
/// Eliminates the syscall / iostream / kernel-copy machinery; keeps exactly one (userspace) copy. Setup is
/// done in open() with the engine's buffer provider, NOT in the ctor (the InMemory ctor preload is an
/// antipattern). Diagnostic source for the source-scaling study; selected via `TYPE MmapFile`.
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

    /// mmap the file.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// munmap + close fd.
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string filePath;
    int fileDescriptor = -1;
    void* mapBase = nullptr; ///< base of the whole-file mapping (page-aligned), or nullptr for an empty file
    size_t mapSize = 0; ///< file size in bytes = mapping length
    size_t cursor = 0; ///< next byte to copy out
    std::atomic<size_t> totalNumBytesRead = 0;
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
