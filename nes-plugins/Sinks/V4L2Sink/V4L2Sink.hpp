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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

struct ConfigParametersV4L2Sink
{
    /// NOLINTBEGIN(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> DEVICE{
        "DEVICE",
        "/dev/video10",
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            const auto device = DescriptorConfig::tryGet(DEVICE, config);
            return device.has_value() && !device->empty() ? device : std::nullopt;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> FRAME_RATE{
        "FRAME_RATE",
        30,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto rate = DescriptorConfig::tryGet(FRAME_RATE, config);
            return rate.has_value() && *rate > 0 ? rate : std::nullopt;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> POLL_TIMEOUT_MS{
        "POLL_TIMEOUT_MS",
        1000,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto timeout = DescriptorConfig::tryGet(POLL_TIMEOUT_MS, config);
            return timeout.has_value() && *timeout > 0 && *timeout <= std::numeric_limits<int>::max() ? timeout : std::nullopt;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, DEVICE, FRAME_RATE, POLL_TIMEOUT_MS);
    /// NOLINTEND(cert-err58-cpp)
};

/// Publishes native V4L2 source tuples as individual frames to an existing video
/// output device, normally v4l2loopback. The first frame fixes the stream format.
/// Drops frames older than the last successfully written timestamp.
class V4L2Sink final : public Sink
{
public:
    static constexpr std::string_view NAME = "V4L2";

    explicit V4L2Sink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~V4L2Sink() override;

    void start(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& stream) const override;

private:
    void configureFormat(uint32_t width, uint32_t height, uint32_t pixelFormat);
    void writeFrame(std::span<const uint8_t> image);
    void close();

    std::string devicePath;
    uint32_t frameRate;
    uint32_t pollTimeoutMs;
    int device{-1};
    uint32_t negotiatedWidth{0};
    uint32_t negotiatedHeight{0};
    uint32_t negotiatedPixelFormat{0};
    uint32_t frameSize{0};
    bool compressed{false};
    uint64_t framesReceived{0};
    uint64_t framesWritten{0};
    uint64_t framesDropped{0};
    std::optional<uint64_t> lastWrittenTimestamp;
    std::chrono::steady_clock::time_point lastFrameLog{};
    std::chrono::steady_clock::time_point lastDropLog{};
    /// Serialize timestamp comparisons and frame writes across worker threads.
    std::mutex mutex;
};

}
