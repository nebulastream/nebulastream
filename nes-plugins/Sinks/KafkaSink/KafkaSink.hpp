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
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <librdkafka/rdkafkacpp.h>
#include <nes-network-bindings/lib.h>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// A sink that produces the bytes of every incoming TupleBuffer as a single Kafka message
/// to the configured topic.
class KafkaSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Kafka";
    static constexpr auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);
    /// How long start() waits for broker metadata before giving up.
    static constexpr int32_t METADATA_TIMEOUT_IN_MILLISECONDS = 5000;

    explicit KafkaSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    class DeliveryReportCallback final : public RdKafka::DeliveryReportCb
    {
    public:
        void dr_cb(RdKafka::Message& message) override;
        std::atomic<RdKafka::ErrorCode> error{RdKafka::ERR_NO_ERROR};
    };

    /// librdkafka frees a RK_MSG_FREE payload with free(3), so such a payload must come from malloc(3).
    struct MallocDeleter
    {
        void operator()(void* pointer) const noexcept;
    };

    using OwnedPayload = std::unique_ptr<void, MallocDeleter>;

    /// Outcome of a single publish attempt:
    ///   Ok   - librdkafka accepted the message into its outbound queue.
    ///   Full - outbound queue is at capacity; caller should buffer & retry.
    enum class PublishResult : uint8_t
    {
        Ok,
        Full,
    };
    PublishResult tryProduce(const TupleBuffer& buffer);

    /// Serializes a buffer and all of its child buffers into one malloc'd block, sized up front so the
    /// bytes are written exactly once.
    static std::pair<OwnedPayload, size_t> serializePayload(const TupleBuffer& buffer);

    /// Payload of the buffer librdkafka last rejected with ERR__QUEUE_FULL, kept so that the
    /// backpressure retry of that buffer does not serialize it a second time.
    /// Keyed by origin/sequence/chunk number: a sequence number is only unique *within* an origin, so
    /// a sink fed by several origins would otherwise publish one buffer's bytes under another buffer's
    /// identity. Synchronized because the engine calls execute() concurrently from several worker threads.
    struct RetryPayload
    {
        OriginId originId = INVALID<OriginId>;
        SequenceNumber sequenceNumber = INVALID<SequenceNumber>;
        ChunkNumber chunkNumber = INVALID<ChunkNumber>;
        OwnedPayload payload;
        size_t payloadSize = 0;
    };

    std::string bootstrapServers;
    std::string topic;
    int32_t maxOutstandingMessages;
    int32_t deliveryTimeoutMs;

    DeliveryReportCallback deliveryReportCallback;
    std::unique_ptr<RdKafka::Producer> producer;
    BackpressureHandler backpressureHandler;
    folly::Synchronized<RetryPayload> retryPayload;
};

struct ConfigParametersKafkaSink
{
    ///NOLINTBEGIN(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> BROKERS{
        "BROKERS",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BROKERS, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "TOPIC",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TOPIC, config); }};

    /// Maximum number of messages librdkafka may hold in its outbound queue (queue.buffering.max.messages).
    /// Once reached, produce() fails with ERR__QUEUE_FULL and the sink retries via the BackpressureHandler.
    /// Must be positive: librdkafka treats 0 as "unlimited", which would disable backpressure.
    static inline const DescriptorConfig::ConfigParameter<int32_t> MAX_OUTSTANDING_MESSAGES{
        "MAX_OUTSTANDING_MESSAGES",
        100000,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int32_t>
        {
            auto value = DescriptorConfig::tryGet(MAX_OUTSTANDING_MESSAGES, config);
            if (!value || value.value() <= 0)
            {
                NES_ERROR("MAX_OUTSTANDING_MESSAGES must be positive, got {}.", value.value_or(-1));
                return std::nullopt;
            }
            return value;
        }};

    /// How long librdkafka retries a produced message before reporting it failed (message.timeout.ms).
    /// Must be positive: librdkafka treats 0 as "infinite", which could block a query forever.
    static inline const DescriptorConfig::ConfigParameter<int32_t> DELIVERY_TIMEOUT_MS{
        "DELIVERY_TIMEOUT_MS",
        5000,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int32_t>
        {
            auto value = DescriptorConfig::tryGet(DELIVERY_TIMEOUT_MS, config);
            if (!value || value.value() <= 0)
            {
                NES_ERROR("DELIVERY_TIMEOUT_MS must be positive, got {}.", value.value_or(-1));
                return std::nullopt;
            }
            return value;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap, BROKERS, TOPIC, MAX_OUTSTANDING_MESSAGES, DELIVERY_TIMEOUT_MS);
    ///NOLINTEND(cert-err58-cpp)
};

}

FMT_OSTREAM(NES::KafkaSink);
