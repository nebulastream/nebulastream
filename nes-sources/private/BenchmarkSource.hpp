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
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::detail
{
class MemorySegment;
}

namespace NES
{

enum class BenchmarkMode : uint8_t
{
    File,
    TmpfsCold,
    TmpfsWarm,
    Memory,
    InPlace
};

class BenchmarkSource final : public Source
{
public:
    static constexpr std::string_view NAME = "Benchmark";

    explicit BenchmarkSource(const SourceDescriptor& sourceDescriptor);
    ~BenchmarkSource() override;

    BenchmarkSource(const BenchmarkSource&) = delete;
    BenchmarkSource& operator=(const BenchmarkSource&) = delete;
    BenchmarkSource(BenchmarkSource&&) = delete;
    BenchmarkSource& operator=(BenchmarkSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// Config-derived fields
    std::string filePath;
    BenchmarkMode mode;
    std::string tmpfsPath;
    bool loop;

    /// Runtime state for File / TmpfsCold / TmpfsWarm modes
    std::ifstream inputFile;

    /// Runtime state for Memory mode
    std::vector<char> memoryBuffer;
    size_t memoryReadOffset = 0;

    /// Runtime state for InPlace mode
    uint8_t* fileData = nullptr;
    size_t fileDataSize = 0;
    std::vector<std::unique_ptr<detail::MemorySegment>> ownedSegments;
    std::vector<TupleBuffer> preBuiltBuffers;
    size_t currentBufferIndex = 0;
    size_t bufferSize = 0;

    /// Runtime state for Tmpfs modes
    std::string tmpfsCopyPath;

    std::atomic<size_t> totalNumBytesRead{0};

    /// Helpers
    FillTupleBufferResult fillFromFile(TupleBuffer& tupleBuffer);
    FillTupleBufferResult fillFromMemory(TupleBuffer& tupleBuffer);
    FillTupleBufferResult fillFromInPlace(TupleBuffer& tupleBuffer);
    void rewindSource();
};

struct ConfigParametersBenchmark
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> MODE{
        "mode",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MODE, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TMPFS_PATH{
        "tmpfs_path",
        std::string("/dev/shm"),
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TMPFS_PATH, config); }};

    static inline const DescriptorConfig::ConfigParameter<bool> LOOP{
        "loop",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(LOOP, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH, MODE, TMPFS_PATH, LOOP);
};

}
