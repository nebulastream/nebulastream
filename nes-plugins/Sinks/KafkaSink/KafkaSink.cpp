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

#include <KafkaSink.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <librdkafka/rdkafkacpp.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Ownership of the block stays with the caller until produce() accepts it.
std::pair<KafkaSink::OwnedPayload, size_t> KafkaSink::serializePayload(const TupleBuffer& buffer)
{
    size_t payloadSize = 0;
    BufferIterator sizeIterator{buffer};
    for (auto element = sizeIterator.getNextElement(); element.has_value(); element = sizeIterator.getNextElement())
    {
        payloadSize += element->contentLength;
    }

    /// malloc(0) may return nullptr, which librdkafka would produce as a *null* message rather than an
    /// empty one; always allocate at least one byte so an empty buffer keeps producing an empty message.
    OwnedPayload payload{std::malloc(std::max<size_t>(payloadSize, 1))}; /// NOLINT(cppcoreguidelines-no-malloc)
    if (not payload)
    {
        throw CannotOpenSink("KafkaSink failed to allocate {} bytes for the outgoing message", payloadSize);
    }

    size_t offset = 0;
    BufferIterator iterator{buffer};
    for (auto element = iterator.getNextElement(); element.has_value(); element = iterator.getNextElement())
    {
        /// Sizing and copying are two independent walks of the same buffer; bound every write rather
        /// than only checking the total afterwards, so a disagreement cannot overflow the block.
        INVARIANT(
            offset + element->contentLength <= payloadSize,
            "KafkaSink payload grew past the {} bytes reserved for it while serializing",
            payloadSize);
        const auto data = element->buffer.getAvailableMemoryArea<char>().first(element->contentLength);
        std::memcpy(static_cast<char*>(payload.get()) + offset, data.data(), element->contentLength);
        offset += element->contentLength;
    }
    INVARIANT(offset == payloadSize, "KafkaSink serialized {} bytes but reserved {}", offset, payloadSize);
    return {std::move(payload), payloadSize};
}

void KafkaSink::MallocDeleter::operator()(void* pointer) const noexcept
{
    std::free(pointer); /// NOLINT(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
}

void KafkaSink::DeliveryReportCallback::dr_cb(RdKafka::Message& message)
{
    if (message.err() != RdKafka::ERR_NO_ERROR)
    {
        error = message.err();
    }
}

KafkaSink::KafkaSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , bootstrapServers(sinkDescriptor.getFromConfig(ConfigParametersKafkaSink::BROKERS))
    , topic(sinkDescriptor.getFromConfig(ConfigParametersKafkaSink::TOPIC))
    , maxOutstandingMessages(sinkDescriptor.getFromConfig(ConfigParametersKafkaSink::MAX_OUTSTANDING_MESSAGES))
    , deliveryTimeoutMs(sinkDescriptor.getFromConfig(ConfigParametersKafkaSink::DELIVERY_TIMEOUT_MS))
    , backpressureHandler(
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_UPPER_THRESHOLD),
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_LOWER_THRESHOLD))
{
}

std::ostream& KafkaSink::toString(std::ostream& os) const
{
    os << "\nKafkaSink(";
    os << "\n  bootstrapServers: " << bootstrapServers;
    os << "\n  topic: " << topic;
    os << "\n  maxOutstandingMessages: " << maxOutstandingMessages;
    os << "\n  deliveryTimeoutMs: " << deliveryTimeoutMs;
    os << ")\n";
    return os;
}

void KafkaSink::start(PipelineExecutionContext&)
{
    NES_INFO("Opening KafkaSink at {} for topic {}.", bootstrapServers, topic);
    auto conf = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    std::string error;
    if (conf->set("bootstrap.servers", bootstrapServers, error) != RdKafka::Conf::CONF_OK
        || conf->set("dr_cb", &deliveryReportCallback, error) != RdKafka::Conf::CONF_OK
        || conf->set("queue.buffering.max.messages", std::to_string(maxOutstandingMessages), error) != RdKafka::Conf::CONF_OK
        || conf->set("message.timeout.ms", std::to_string(deliveryTimeoutMs), error) != RdKafka::Conf::CONF_OK)
    {
        throw CannotOpenSink("Failed to configure Kafka producer for brokers {}: {}", bootstrapServers, error);
    }

    producer.reset(RdKafka::Producer::create(conf.get(), error));
    if (!producer)
    {
        throw CannotOpenSink("Failed to create Kafka producer for brokers {}: {}", bootstrapServers, error);
    }

    /// Producer creation is always local and does not contact the brokers; a metadata request
    /// is the only way to detect an unreachable cluster before the first execute() call.
    RdKafka::Metadata* metadataRaw = nullptr;
    const auto err = producer->metadata(false, nullptr, &metadataRaw, METADATA_TIMEOUT_IN_MILLISECONDS);
    const std::unique_ptr<RdKafka::Metadata> metadata(metadataRaw);
    if (err != RdKafka::ERR_NO_ERROR)
    {
        throw CannotOpenSink("Failed to reach Kafka brokers {}: {}", bootstrapServers, RdKafka::err2str(err));
    }
}

KafkaSink::PublishResult KafkaSink::tryProduce(const TupleBuffer& buffer)
{
    /// librdkafka only releases a produce()'d message's queue slot once its delivery report has
    /// been popped via poll() se we need to poll on every try (repeat or new one)
    producer->poll(0);

    /// Reuse the payload if this is the retry of the buffer librdkafka last rejected.
    OwnedPayload payload;
    size_t payloadSize = 0;
    {
        const auto pending = retryPayload.wlock();
        if (pending->payload && pending->originId == buffer.getOriginId() && pending->sequenceNumber == buffer.getSequenceNumber()
            && pending->chunkNumber == buffer.getChunkNumber())
        {
            payload = std::move(pending->payload);
            payloadSize = pending->payloadSize;
        }
    }
    if (not payload)
    {
        std::tie(payload, payloadSize) = serializePayload(buffer);
    }

    /// RK_MSG_FREE hands the serialized block to librdkafka instead of letting it copy the payload a
    /// second time; librdkafka takes ownership only if produce() succeeds.
    /// Ordering: an unassigned partition and a null key spread consecutive buffers over the
    /// topic's partitions, so only per-partition order is guaranteed - see the class comment for why a
    /// fixed partition or a stable key would not restore stream order.
    const auto err = producer->produce(
        topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_FREE, payload.get(), payloadSize, nullptr, 0, 0, nullptr, nullptr);

    if (err == RdKafka::ERR__QUEUE_FULL)
    {
        /// produce() failed, so the payload is still ours: keep it for the retry of this buffer.
        const auto pending = retryPayload.wlock();
        pending->originId = buffer.getOriginId();
        pending->sequenceNumber = buffer.getSequenceNumber();
        pending->chunkNumber = buffer.getChunkNumber();
        pending->payloadSize = payloadSize;
        pending->payload = std::move(payload);
        return PublishResult::Full;
    }
    if (err != RdKafka::ERR_NO_ERROR)
    {
        throw CannotOpenSink("KafkaSink produce to topic {} failed: {}", topic, RdKafka::err2str(err));
    }
    /// Accepted: librdkafka owns the block now and frees it once the message is delivered.
    std::ignore = payload.release();
    return PublishResult::Ok;
}

void KafkaSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pec)
{
    PRECONDITION(producer, "KafkaSink producer is not initialized");
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in KafkaSink.");

    if (const auto error = deliveryReportCallback.error.load(); error != RdKafka::ERR_NO_ERROR)
    {
        throw CannotOpenSink("KafkaSink delivery to brokers {} failed: {}", bootstrapServers, RdKafka::err2str(error));
    }

    auto currentBuffer = std::optional{inputTupleBuffer};
    while (currentBuffer)
    {
        switch (tryProduce(*currentBuffer))
        {
            case PublishResult::Ok: {
                currentBuffer = backpressureHandler.onSuccess(backpressureController);
                continue;
            }
            case PublishResult::Full: {
                if (const auto emit = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*emit, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
            }
        }
    }
}

void KafkaSink::stop(PipelineExecutionContext& pec)
{
    if (!producer)
    {
        return;
    }
    INVARIANT(backpressureHandler.empty(), "BackpressureHandler is not empty");

    producer->poll(0);
    if (producer->outq_len() > 0)
    {
        pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
        return;
    }
    if (const auto error = deliveryReportCallback.error.load(); error != RdKafka::ERR_NO_ERROR)
    {
        throw CannotOpenSink("KafkaSink delivery to brokers {} failed: {}", bootstrapServers, RdKafka::err2str(error));
    }
    NES_INFO("Kafka Sink completed.");
    producer.reset();
    /// Drop any payload still held for a retry: sequence numbers restart from the beginning if this
    /// sink is started again, so a stale entry could be matched by an unrelated buffer.
    *retryPayload.wlock() = {};
}

DescriptorConfig::Config KafkaSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersKafkaSink>(std::move(config), NAME);
}

}
