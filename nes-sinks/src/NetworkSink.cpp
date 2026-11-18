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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <shared_mutex>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
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
#include <ReconfigurationState.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(bugprone-throwing-static-initialization)
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

/// NOLINTEND(bugprone-throwing-static-initialization)

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

namespace
{
NetworkServiceOptions senderOptions(const size_t senderQueueSize, const size_t maxPendingAcks)
{
    return NetworkServiceOptions{
        .sender_queue_size = static_cast<uint32_t>(senderQueueSize),
        .max_pending_acks = static_cast<uint32_t>(maxPendingAcks),
        .receiver_queue_size = 0,
    };
}

constexpr auto FLUSH_POLL_INTERVAL = std::chrono::milliseconds(1);
constexpr auto FLUSH_TIMEOUT = std::chrono::seconds(60);

rust::Slice<const uint8_t> asBytes(const std::string& string)
{
    return {reinterpret_cast<const uint8_t*>(string.data()), string.size()}; ///NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}
}

void NetworkSink::start(PipelineExecutionContext& pec)
{
    this->server = sender_instance(thisConnection);
    this->channel
        = register_sender_channel(*server.value(), connectionAddr, rust::String(channelId), senderOptions(senderQueueSize, maxPendingAcks));
    NES_DEBUG("Sender channel registered: {} (SENDER_QUEUE_SIZE: {}, MAX_PENDING_ACKS: {})", channelId, senderQueueSize, maxPendingAcks);
    reconfigurationState = &pec.getReconfigurationState();
    reconfigurationState->registerSinkUpdateHandler(
        channelId,
        this,
        [this](const NetworkSinkUpdate& update, const ReconfigurationMarker& marker) { return connectToNewReceiver(update, marker); });
}

std::string NetworkSink::connectToNewReceiver(const NetworkSinkUpdate& update, const ReconfigurationMarker& marker)
{
    const std::unique_lock lock(channelMutex);
    if (closed or not channel)
    {
        NES_WARNING("Sender channel {} is closed and cannot connect to channel {}", channelId, update.newChannel);
        return channelId;
    }
    if (update.oldChannel != channelId)
    {
        NES_WARNING("Sender channel {} received an update for channel {}", channelId, update.oldChannel);
    }

    /// Everything the old receiver accepts belongs to the old path. Wait until it was sent, unless the old receiver left.
    if (not detached)
    {
        const auto deadline = std::chrono::steady_clock::now() + FLUSH_TIMEOUT;
        for (auto flushed = false; not flushed;)
        {
            switch (flush_sender_channel(*channel.value()))
            {
                case FlushResult::Flushed:
                    flushed = true;
                    break;
                case FlushResult::Detached:
                    detached = true;
                    flushed = true;
                    break;
                case FlushResult::Failed:
                    closed = true;
                    throw CannotWriteToSink("NetworkSink channel {} failed permanently while connecting to a new receiver", channelId);
                case FlushResult::Pending:
                    if (std::chrono::steady_clock::now() > deadline)
                    {
                        throw CannotWriteToSink("NetworkSink channel {} could not be flushed within {}", channelId, FLUSH_TIMEOUT);
                    }
                    std::this_thread::sleep_for(FLUSH_POLL_INTERVAL);
                    break;
            }
        }
    }

    if (detached)
    {
        try
        {
            redirect_sender_channel(*channel.value(), rust::String(update.newDataEndpoint), rust::String(update.newChannel));
        }
        catch (const rust::Error& error)
        {
            closed = true;
            throw CannotWriteToSink("NetworkSink channel {} could not be redirected to {}: {}", channelId, update.newChannel, error.what());
        }
        detached = false;
        NES_INFO("Redirected Sender channel {} to channel {} on {}", channelId, update.newChannel, update.newDataEndpoint);
    }
    else
    {
        /// The old receiver gets the marker after all data that took the old path.
        const auto serializedMarker = marker.serialize();
        close_sender_channel_with_marker(*std::move(this->channel), asBytes(serializedMarker));
        this->channel = register_sender_channel(
            *server.value(),
            rust::String(update.newDataEndpoint),
            rust::String(update.newChannel),
            senderOptions(senderQueueSize, maxPendingAcks));
        NES_INFO(
            "Sender channel {} sent marker {} and connected to channel {} on {}",
            channelId,
            marker.getId(),
            update.newChannel,
            update.newDataEndpoint);
    }
    channelId = update.newChannel;
    connectionAddr = update.newDataEndpoint;
    return channelId;
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

    /// No update may connect the sink to a new receiver while it stops. Unregister before locking the channel, as the reconfiguration state
    /// holds its lock while it hands an update to the sink.
    auto& state = pec.getReconfigurationState();
    state.unregisterSinkUpdateHandlers(this);
    const std::unique_lock lock(channelMutex);
    const auto termination = state.getChannelTermination(channelId);

    if (!closed)
    {
        if (detached)
        {
            /// The receiver left and nobody connects the sink to a new receiver anymore, as its query ends.
            const auto dropped = backpressureHandler.clear(backpressureController);
            NES_WARNING("Receiver of Sender channel {} detached and the sink stops, dropping {} buffered buffers", channelId, dropped);
            close_sender_channel(*std::move(this->channel));
            return;
        }
        INVARIANT(backpressureHandler.empty(), "BackpressureHandler is not empty");

        switch (flush_sender_channel(*this->channel.value()))
        {
            case FlushResult::Flushed: {
                break;
            }
            case FlushResult::Pending: {
                /// The sender network service has pending buffers to send, keep the pipeline alive by emitting an empty buffer
                pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
                return;
            }
            case FlushResult::Failed: {
                /// Already sent buffers are lost. Stopping regularly would silently drop them.
                this->closed = true;
                throw CannotWriteToSink("NetworkSink channel failed permanently");
            }
            case FlushResult::Detached: {
                /// The receiver left while the sink stops: it was drained before this query. The data it did not take is lost.
                NES_WARNING("Receiver of Sender channel {} detached while the sink stops", channelId);
                close_sender_channel(*std::move(this->channel));
                return;
            }
        }
    }

    /// A drained query forwards its marker to receivers that are drained as well and detaches from receivers that keep running.
    switch (termination)
    {
        case ChannelTermination::ForwardMarker: {
            const auto marker = state.getDrainMarker();
            INVARIANT(marker.has_value(), "A drained query has a drain marker");
            NES_DEBUG("Closing Sender channel {} with reconfiguration marker {}", channelId, marker->getId());
            const auto serializedMarker = marker->serialize();
            close_sender_channel_with_marker(*std::move(this->channel), asBytes(serializedMarker));
            return;
        }
        case ChannelTermination::Detach: {
            NES_DEBUG("Detaching Sender channel {}", channelId);
            detach_sender_channel(*std::move(this->channel));
            return;
        }
        case ChannelTermination::Close: {
            NES_DEBUG("Closing Sender channel {}", channelId);
            close_sender_channel(*std::move(this->channel));
            NES_DEBUG("Sender channel {} closed", channelId);
            return;
        }
    }
}

void NetworkSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in NetworkSink.");

    if (closed)
    {
        NES_WARNING("Sink is closed dropping buffer: {}-{}", inputBuffer.getSequenceNumber(), inputBuffer.getChunkNumber());
        return;
    }

    /// While the sink connects to a new receiver, it keeps the data and retries later instead of blocking the worker thread.
    const std::shared_lock lock(channelMutex, std::try_to_lock);
    if (detached and pec.getReconfigurationState().isStopped())
    {
        /// Nobody connects the sink to a new receiver anymore. Ending the retries lets the stopped query terminate.
        const auto dropped = backpressureHandler.clear(backpressureController) + 1;
        NES_WARNING("Sender channel {} waits for a new receiver, but its query was stopped. Dropping {} buffers", channelId, dropped);
        return;
    }
    if (not lock.owns_lock() or detached)
    {
        if (const auto emit = backpressureHandler.onFull(inputBuffer, backpressureController))
        {
            pec.repeatTask(*emit, BACKPRESSURE_RETRY_INTERVAL);
        }
        return;
    }
    PRECONDITION(channel, "Sender channel is not initialized");

    auto currentBuffer = std::optional(inputBuffer);
    while (currentBuffer)
    {
        /// Set buffer header
        const SerializedTupleBufferHeader metadata{
            .sequence_number = currentBuffer->getSequenceNumber().getRawValue(),
            .origin_id = currentBuffer->getOriginId().getRawValue(),
            .chunk_number = currentBuffer->getChunkNumber().getRawValue(),
            .number_of_tuples = currentBuffer->getNumberOfTuples(),
            .watermark = currentBuffer->getWatermark().getRawValue(),
            .last_chunk = currentBuffer->isLastChunk()};

        /// Set child buffers
        std::vector<rust::Slice<const uint8_t>> children;
        children.reserve(currentBuffer->getNumberOfChildBuffers());
        for (size_t childIdx = 0; childIdx < currentBuffer->getNumberOfChildBuffers(); ++childIdx)
        {
            auto childBuffer = currentBuffer->loadChildBuffer(ChildBufferIndex(childIdx));
            auto childMemory = childBuffer.getAvailableMemoryArea<const uint8_t>();
            const auto usedChildBytes = childBuffer.getNumberOfTuples();
            const auto childPayload
                = (usedChildBytes > 0 and usedChildBytes <= childMemory.size()) ? childMemory.subspan(0, usedChildBytes) : childMemory;
            children.emplace_back(childPayload);
        }

        std::span usedBufferMemory(
            currentBuffer->getAvailableMemoryArea<const uint8_t>().data(), currentBuffer->getNumberOfTuples() * tupleSize);
        /// Set data and send over the network
        const auto sendResult = send_buffer(
            *channel.value(), metadata, rust::Slice(usedBufferMemory), rust::Slice<const rust::Slice<const uint8_t>>(children));
        switch (sendResult)
        {
            case SendResult::Closed: {
                /// Future buffers are voided.
                this->closed = true;
                [[maybe_unused]] auto droppedBuffer = backpressureHandler.onFull(*currentBuffer, backpressureController);
                /// Currently there is no way to propagate a query stop without a failure from a sink.
                /// There is no operator that propagates a query stop in upstream direction, so receiving a query stop
                /// from the downstream operator is unexpected, thus failing the query is reasonable.
                throw CannotWriteToSink("NetworkSink channel was closed by the other side or failed permanently");
            }
            case SendResult::Detached: {
                /// The receiver was drained (ISQD). The sender keeps the data it did not take, and the sink buffers new data, until the
                /// sink is connected to a new receiver.
                if (not detached.exchange(true))
                {
                    NES_INFO("Receiver of Sender channel {} detached, buffering until the sink is connected to a new receiver", channelId);
                }
                if (const auto emit = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*emit, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
            }
            case SendResult::Ok: {
                NES_TRACE("Sending buffer {}", currentBuffer->getSequenceNumber());
                /// Sent a buffer, check the backpressure handler to send another one
                currentBuffer = backpressureHandler.onSuccess(*currentBuffer, backpressureController);
                break;
            }
            case SendResult::Full: {
                if (const auto emit = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*emit, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
            }
        }
    }
}

std::ostream& NetworkSink::toString(std::ostream& str) const
{
    return str << fmt::format("NetworkSink(connectionId: {}, channelId: {})", connectionAddr, channelId);
}

}
