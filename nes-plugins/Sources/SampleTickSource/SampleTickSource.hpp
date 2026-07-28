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

#include <chrono>
#include <cstdint>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

class SampleTickSource final : public Source
{
public:
    static constexpr std::string_view NAME = "SampleTick";

    explicit SampleTickSource(const SourceDescriptor& sourceDescriptor);
    ~SampleTickSource() override = default;

    SampleTickSource(const SampleTickSource&) = delete;
    SampleTickSource& operator=(const SampleTickSource&) = delete;
    SampleTickSource(SampleTickSource&&) = delete;
    SampleTickSource& operator=(SampleTickSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    uint64_t slotDurationMs;
    int32_t maxRuntimeMs;
    int64_t maxTicks;
    uint64_t tickCounter{0};
    uint64_t generatedBuffers{0};
    std::chrono::time_point<std::chrono::steady_clock> startOfSlot;
    std::chrono::time_point<std::chrono::steady_clock> sourceStartTime;
};

struct ConfigParametersSampleTick
{
    static inline const DescriptorConfig::ConfigParameter<uint64_t> SLOT_DURATION_MS{
        "SLOT_DURATION_MS",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SLOT_DURATION_MS, config); }};

    /// If set to -1 the source ticks indefinitely until stopped by another thread. Mirrors GeneratorSource
    static inline const DescriptorConfig::ConfigParameter<int32_t> MAX_RUNTIME_MS{
        "MAX_RUNTIME_MS",
        -1,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_RUNTIME_MS, config); }};

    /// If set to -1 the source ticks indefinitely (subject only to MAX_RUNTIME_MS).
    static inline const DescriptorConfig::ConfigParameter<int64_t> MAX_TICKS{
        "MAX_TICKS",
        -1,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_TICKS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, SLOT_DURATION_MS, MAX_RUNTIME_MS, MAX_TICKS);
};

}
