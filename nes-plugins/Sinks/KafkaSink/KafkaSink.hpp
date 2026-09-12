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
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <librdkafka/rdkafkacpp.h>
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
    /// Bounds how long start() waits for broker metadata and how long a queued message may
    /// be retried before librdkafka reports it as failed via the delivery report callback.
    /// Without a bound, an unreachable broker would hang start() indefinitely and a downed
    /// broker mid-query would sit in librdkafka's internal retry queue rather than failing.
    static constexpr int32_t BROKER_TIMEOUT_IN_MILLISECONDS = 5000;

    explicit KafkaSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    /// Outcome of a single publish attempt. Mirrors NetworkSink's `SendResult`:
    ///   Ok     - librdkafka accepted the message into its outbound queue.
    ///   Full   - outbound queue is at capacity; caller should buffer & retry.
    ///   Closed - non-recoverable prducer error (disconnected, protocol, etc.); caller should fail the query.
    enum class ProduceResult : uint8_t
    {
        Ok,
        Full,
        Closed,
    };

    class DeliveryReportCallback final : public RdKafka::DeliveryReportCb
    {
    public:
        void dr_cb(RdKafka::Message& message) override;
        std::atomic<bool> failed{false};
        std::string lastError;
    };

    ProduceResult tryProduce(const TupleBuffer& buffer);

    std::string brokers;
    std::string topic;
    int32_t maxOutstandingMessages;

    DeliveryReportCallback deliveryReportCallback;
    std::unique_ptr<RdKafka::Producer> producer;
    BackpressureHandler backpressureHandler;
};

/// Defines the names, (optional) default values, (optional) validation & config functions for all Kafka sink config parameters.
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
    static inline const DescriptorConfig::ConfigParameter<int32_t> MAX_OUTSTANDING_MESSAGES{
        "MAX_OUTSTANDING_MESSAGES",
        100000,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(MAX_OUTSTANDING_MESSAGES, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, BROKERS, TOPIC, MAX_OUTSTANDING_MESSAGES);
    ///NOLINTEND(cert-err58-cpp)
};

}

FMT_OSTREAM(NES::KafkaSink);
