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

#include <Sinks/NetworkSink.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <ostream>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <nes-network-bindings/lib.h>
#include <rust/cxx.h>

#include <Configurations/ConfigLiteral.hpp>
#include <DataTypes/UnboundField.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(bugprone-throwing-static-initialization,cert-err58-cpp)
const ConfigField<std::string> DATA_ENDPOINT{
    Identifier::parse("DATA_ENDPOINT"), "The data-plane host:port of the worker with corresponding network source."};

const ConfigField<std::string> BIND{
    Identifier::parse("BIND"), "The data-plane host:port of this worker. Must match the data address the worker was started with."};

const ConfigField<std::string> CHANNEL{Identifier::parse("CHANNEL"), "The UUID of the network channel."};

const ConfigField<size_t> SENDER_QUEUE_SIZE{
    Identifier::parse("SENDER_QUEUE_SIZE"),
    "Per-channel sender queue size override. 0 means use the worker-level default.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<size_t>()).and_then(narrowConfigValue<int64_t, size_t>); },
    size_t{0}};

const ConfigField<size_t> MAX_PENDING_ACKS{
    Identifier::parse("MAX_PENDING_ACKS"),
    "Per-channel max pending acks override. 0 means use the worker-level default.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<size_t>()).and_then(narrowConfigValue<int64_t, size_t>); },
    size_t{0}};

/// NOLINTEND(bugprone-throwing-static-initialization,cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> NetworkSink::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("NETWORK_SINK"),
        DATA_ENDPOINT,
        BIND,
        CHANNEL,
        SENDER_QUEUE_SIZE,
        MAX_PENDING_ACKS,
        SinkDescriptor::BACKPRESSURE_UPPER_THRESHOLD,
        SinkDescriptor::BACKPRESSURE_LOWER_THRESHOLD);
}

std::expected<NetworkSinkConfig, Exception> NetworkSinkConfig::fromConfig(const InstantiatedConfig& config)
{
    return NetworkSinkConfig{
        .dataEndpoint = config.get(DATA_ENDPOINT),
        .bind = config.get(BIND),
        .channel = config.get(CHANNEL),
        .senderQueueSize = config.get(SENDER_QUEUE_SIZE),
        .maxPendingAcks = config.get(MAX_PENDING_ACKS)};
}

NetworkSink::NetworkSink(
    BackpressureController backpressureController, const NetworkSinkConfig& config, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , tupleSize(getSizeInBytes(*NES::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptor.getSchema())))
    , backpressureHandler(sinkDescriptor.getBackpressureUpperThreshold(), sinkDescriptor.getBackpressureLowerThreshold())
    , channelId(config.channel)
    , connectionAddr(config.dataEndpoint)
    , thisConnection(config.bind)
    , senderQueueSize(config.senderQueueSize)
    , maxPendingAcks(config.maxPendingAcks)
{
}

void NetworkSink::start(PipelineExecutionContext&)
{
    this->server = sender_instance(thisConnection);
    const NetworkServiceOptions options{
        .sender_queue_size = static_cast<uint32_t>(senderQueueSize),
        .max_pending_acks = static_cast<uint32_t>(maxPendingAcks),
        .receiver_queue_size = 0,
    };
    this->channel = register_sender_channel(*server.value(), connectionAddr, rust::String(channelId), options);
    NES_DEBUG("Sender channel registered: {} (SENDER_QUEUE_SIZE: {}, MAX_PENDING_ACKS: {})", channelId, senderQueueSize, maxPendingAcks);
}

void NetworkSink::stop(PipelineExecutionContext& pec)
{
    /// A pipeline can be stopped without ever having been started, e.g. when the deployment of a query is cancelled because another
    /// pipeline failed to start. Nothing was registered, so there is nothing to flush or close.
    if (!channel)
    {
        NES_DEBUG("Sender channel {} was never registered, nothing to close", channelId);
        return;
    }

    /// Once the queued buffers are sent, the sender network service still has to send its own pending buffers.
    if (!backpressureHandler.stop(
            backpressureController,
            [this](const TupleBuffer& buffer) { return trySend(buffer); },
            [this] { return flush_sender_channel(*this->channel.value()); }))
    {
        /// Not bounded: the receiving side may legitimately take long to register its channel or to consume.
        pec.repeatTask({}, BackpressureHandler::RETRY_INTERVAL);
        return;
    }

    NES_DEBUG("Closing Sender channel {}", channelId);
    close_sender_channel(*std::move(this->channel));
    NES_DEBUG("Sender channel {} closed", channelId);
}

bool NetworkSink::trySend(const TupleBuffer& buffer)
{
    /// Set buffer header
    const SerializedTupleBufferHeader metadata{
        .sequence_number = buffer.getSequenceNumber().getRawValue(),
        .origin_id = buffer.getOriginId().getRawValue(),
        .chunk_number = buffer.getChunkNumber().getRawValue(),
        .number_of_tuples = buffer.getNumberOfTuples(),
        .watermark = buffer.getWatermark().getRawValue(),
        .last_chunk = buffer.isLastChunk()};

    /// Set child buffers
    std::vector<rust::Slice<const uint8_t>> children;
    children.reserve(buffer.getNumberOfChildBuffers());
    for (size_t childIdx = 0; childIdx < buffer.getNumberOfChildBuffers(); ++childIdx)
    {
        auto childBuffer = buffer.loadChildBuffer(ChildBufferIndex(childIdx));
        auto childMemory = childBuffer.getAvailableMemoryArea<const uint8_t>();
        const auto usedChildBytes = childBuffer.getNumberOfTuples();
        const auto childPayload
            = (usedChildBytes > 0 and usedChildBytes <= childMemory.size()) ? childMemory.subspan(0, usedChildBytes) : childMemory;
        children.emplace_back(childPayload);
    }

    std::span usedBufferMemory(buffer.getAvailableMemoryArea<const uint8_t>().data(), buffer.getNumberOfTuples() * tupleSize);
    /// Set data and send over the network
    switch (send_buffer(*channel.value(), metadata, rust::Slice(usedBufferMemory), rust::Slice<const rust::Slice<const uint8_t>>(children)))
    {
        case SendResult::Ok:
            NES_TRACE("Sent buffer {}", buffer.getSequenceNumber());
            return true;
        case SendResult::Full:
            return false;
        case SendResult::Closed:
            /// Currently there is no way to propagate a query stop without a failure from a sink.
            /// There is no operator that propagates a query stop in upstream direction, so receiving a query stop
            /// from the downstream operator is unexpected, thus failing the query is reasonable.
            throw CannotWriteToSink("NetworkSink was closed by the other side");
    }
    std::unreachable();
}

void NetworkSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec)
{
    PRECONDITION(channel, "Sender channel is not initialized");
    PRECONDITION(inputBuffer, "Invalid input buffer in NetworkSink.");

    if (const auto retry
        = backpressureHandler.send(inputBuffer, backpressureController, [this](const TupleBuffer& buffer) { return trySend(buffer); }))
    {
        pec.repeatTask(*retry, BackpressureHandler::RETRY_INTERVAL);
    }
}

std::ostream& NetworkSink::toString(std::ostream& str) const
{
    return str << fmt::format("NetworkSink(connectionId: {}, channelId: {})", connectionAddr, channelId);
}

}
