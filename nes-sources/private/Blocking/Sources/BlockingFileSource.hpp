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

static constexpr std::string_view SYSTEST_FILE_PATH_PARAMETER = "file_path";

class BlockingFileSource final : public BlockingSource
{
public:
    static constexpr std::string_view NAME = "File";

    explicit BlockingFileSource(const SourceDescriptor& sourceDescriptor);
    ~BlockingFileSource() override = default;

    BlockingFileSource(const BlockingFileSource&) = delete;
    BlockingFileSource& operator=(const BlockingFileSource&) = delete;
    BlockingFileSource(BlockingFileSource&&) = delete;
    BlockingFileSource& operator=(BlockingFileSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken, size_t offset) override;

    /// Open file socket.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// Close file socket.
    void close() override;

    /// validates and formats a string to string configuration
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// Raw file descriptor read directly into the pool TupleBuffer via ::read (no std::ifstream: its
    /// filebuf double-buffers -> an extra page-cache->filebuf->dst __memmove + the _IO_* machinery,
    /// ~16% of the source thread; a plain read leaves only the one unavoidable page-cache->dst copy,
    /// exactly like the io_uring/File sources). The formatter string_views into this buffer as before.
    int fileDescriptor = -1;
    std::string filePath;
    std::atomic<size_t> totalNumBytesRead;
    /// PROFILING-only (env NES_FILE_REPEAT=N, default 1): on EOF, seek back to 0 and re-read the file
    /// N passes back-to-back so latency/throughput sees a long steady window (mirrors the async
    /// FileSource; no clean repeat mechanism otherwise). One mis-parsed row per pass seam, negligible.
    std::size_t numPasses = 1;
    std::size_t passesDone = 0;
};

struct ConfigParametersCSV
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string(SYSTEST_FILE_PATH_PARAMETER),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

}
