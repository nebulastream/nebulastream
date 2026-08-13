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
#include <expected>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <librdkafka/rdkafkacpp.h>
#include <nes-network-bindings/lib.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

struct KafkaSinkConfig
{
    std::string bootstrapServers;
    std::string topic;
    int64_t maxOutstandingMessages{};
    int64_t deliveryTimeoutMs{};

    static std::expected<KafkaSinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// A sink that produces the bytes of every incoming TupleBuffer as a single Kafka message
/// to the configured topic.
class KafkaSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Kafka";
    static constexpr auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);
    /// How long start() waits for broker metadata before giving up.
    static constexpr int32_t METADATA_TIMEOUT_IN_MILLISECONDS = 5000;

    explicit KafkaSink(BackpressureController backpressureController, const KafkaSinkConfig& config, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    class DeliveryReportCallback final : public RdKafka::DeliveryReportCb
    {
    public:
        void dr_cb(RdKafka::Message& message) override;
        std::atomic<RdKafka::ErrorCode> error{RdKafka::ERR_NO_ERROR};
    };

    /// Outcome of a single publish attempt:
    ///   Ok     - librdkafka accepted the message into its outbound queue.
    ///   Closed - non-recoverable producer error (disconnected, protocol, etc.); caller should fail the query.
    ///   Full   - outbound queue is at capacity; caller should buffer & retry.
    SendResult tryProduce(const TupleBuffer& buffer);

    std::string bootstrapServers;
    std::string topic;
    int32_t maxOutstandingMessages;
    int32_t deliveryTimeoutMs;

    DeliveryReportCallback deliveryReportCallback;
    std::unique_ptr<RdKafka::Producer> producer;
    BackpressureHandler backpressureHandler;
};

}

FMT_OSTREAM(NES::KafkaSink);
