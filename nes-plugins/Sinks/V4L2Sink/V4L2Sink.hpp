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
#include <expected>
#include <limits>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

struct V4L2SinkConfig
{
    std::string device;
    uint32_t frameRate;
    uint32_t pollTimeoutMs;

    static std::expected<V4L2SinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Publishes native V4L2 source tuples as individual frames to an existing video
/// output device, normally v4l2loopback. The first frame fixes the stream format.
/// Drops frames older than the last successfully written timestamp.
class V4L2Sink final : public Sink
{
public:
    static constexpr std::string_view NAME = "V4L2";

    V4L2Sink(BackpressureController backpressureController, const V4L2SinkConfig& config, const SinkDescriptor& sinkDescriptor);
    ~V4L2Sink() override;

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    BufferResult executeBuffer(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;
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
