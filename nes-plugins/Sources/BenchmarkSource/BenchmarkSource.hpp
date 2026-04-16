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
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// Config parameters for the BenchmarkSource plugin.
struct ConfigParametersBenchmark
{
    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> MODE{
        "mode",
        std::string("preload"),
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            auto value = DescriptorConfig::tryGet(MODE, config);
            if (value.has_value() && value.value() != "preload" && value.value() != "mmap")
            {
                return std::nullopt;
            }
            return value;
        }};

    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH, MODE);
};

/// A source optimized for benchmarking that supports two data loading modes:
/// - preload: reads the entire file into memory on open(), serves from buffer
/// - mmap: memory-maps the file, letting the OS handle page faults and caching
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
    std::string filePath;
    std::string mode;
    size_t fileSize = 0;
    size_t readOffset = 0;
    std::atomic<size_t> totalNumBytesRead{0};

    /// Preload mode: file contents loaded into memory
    std::vector<char> preloadedData;

    /// Mmap mode: memory-mapped file
    void* mmapBase = nullptr;
    int mmapFd = -1;
};

}
