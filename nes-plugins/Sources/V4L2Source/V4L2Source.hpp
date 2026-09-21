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

#include <cstddef>
#include <cstdint>
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

struct ConfigParametersV4L2
{
    static inline const DescriptorConfig::ConfigParameter<std::string> DEVICE{
        "DEVICE",
        "/dev/video0",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(DEVICE, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> WIDTH{
        "WIDTH", 320, [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(WIDTH, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> HEIGHT{
        "HEIGHT", 240, [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HEIGHT, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> FRAME_RATE{
        "FRAME_RATE",
        30,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FRAME_RATE, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> PIXEL_FORMAT{
        "PIXEL_FORMAT",
        "MJPG",
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            const auto format = DescriptorConfig::tryGet(PIXEL_FORMAT, config);
            return format.has_value() && format->size() == 4 ? format : std::nullopt;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> BUFFER_COUNT{
        "BUFFER_COUNT",
        4,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto count = DescriptorConfig::tryGet(BUFFER_COUNT, config);
            return count.has_value() && *count >= 2 ? count : std::nullopt;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> POLL_TIMEOUT_MS{
        "POLL_TIMEOUT_MS",
        1000,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(POLL_TIMEOUT_MS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, DEVICE, WIDTH, HEIGHT, FRAME_RATE, PIXEL_FORMAT, BUFFER_COUNT, POLL_TIMEOUT_MS);
};

class V4L2Source final : public Source
{
public:
    static constexpr std::string_view NAME = "V4L2";

    explicit V4L2Source(const SourceDescriptor& sourceDescriptor);
    ~V4L2Source() override;

    V4L2Source(const V4L2Source&) = delete;
    V4L2Source& operator=(const V4L2Source&) = delete;
    V4L2Source(V4L2Source&&) = delete;
    V4L2Source& operator=(V4L2Source&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& stream) const override;

private:
    struct MappedBuffer
    {
        void* data;
        size_t size;
    };

    std::string devicePath;
    uint32_t requestedWidth;
    uint32_t requestedHeight;
    uint32_t requestedFrameRate;
    uint32_t requestedPixelFormat;
    uint32_t requestedBufferCount;
    uint32_t pollTimeoutMs;

    uint32_t negotiatedWidth{0};
    uint32_t negotiatedHeight{0};
    uint32_t negotiatedPixelFormat{0};
    int device{-1};
    bool streaming{false};
    std::vector<MappedBuffer> mappedBuffers;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
};

}
