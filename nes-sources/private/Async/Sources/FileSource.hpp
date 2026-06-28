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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>

#include <Configurations/Descriptor.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceDescriptor.hpp>

// #include <SystestSources/SourceTypes.hpp>

namespace NES
{

static constexpr std::string_view SYSTEST_FILE_PATH_PARAMETER = "file_path";

class FileSource final : public AsyncSource
{
public:
    static inline const std::string NAME = "File";

    explicit FileSource(const SourceDescriptor& sourceDescriptor);
    FileSource() = delete;
    ~FileSource() override = default;

    FileSource(const FileSource&) = delete;
    FileSource& operator=(const FileSource&) = delete;

    FileSource(FileSource&&) = delete;
    FileSource& operator=(FileSource&&) = delete;

    asio::awaitable<InternalSourceResult, Executor> fillBuffer(TupleBuffer& buffer) override;

    /// Open file stream (this source fills the runner-provided buffer one at a time, so it ignores the pool).
    asio::awaitable<void, Executor> open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// Close file stream
    void close() override;

    /// validates and formats a string to string configuration
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    const std::string filePath;
    std::optional<int32_t> fileDescriptor;
    std::optional<asio::posix::stream_descriptor> fileStream;
    /// PROFILING-only (env NES_FILE_REPEAT=N, default 1): on EOF, lseek back to 0 and re-read the file N passes
    /// back-to-back so a profiler sees a long steady window (no clean repeat mechanism otherwise; mirrors the
    /// InMemory blob/prefill multi-pass). Single-pass cost unchanged at N==1. Throughput = reported * numPasses.
    std::size_t numPasses = 1;
    std::size_t passesDone = 0;
};

struct ConfigParametersFileSource
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string(SYSTEST_FILE_PATH_PARAMETER),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

}
