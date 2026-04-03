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
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Encoders/Encoder.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <network/lib.h>
#include <rust/cxx.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

BackpressureHandler::BackpressureHandler(size_t upperThreshold, size_t lowerThreshold)
    : upperThreshold(upperThreshold), lowerThreshold(lowerThreshold)
{
    if (this->lowerThreshold > this->upperThreshold)
    {
        NES_WARNING("Lower threshold is greater than upper threshold. Setting lower threshold to upper threshold.");
        std::swap(this->lowerThreshold, this->upperThreshold);
    }
}

std::optional<TupleBuffer> BackpressureHandler::onFull(TupleBuffer buffer, BackpressureController& backpressureController)
{
    auto rstate = stateLock.ulock();

    /// If this is the pending retry buffer, re-emit it to keep the retry loop alive.
    if (buffer.getSequenceNumber() == rstate->pendingSequenceNumber && buffer.getChunkNumber() == rstate->pendingChunkNumber)
    {
        return buffer;
    }

    const auto wstate = rstate.moveFromUpgradeToWrite();
    wstate->buffered.emplace_back(std::move(buffer));

    /// Apply backpressure when the buffer count reaches the upper hysteresis threshold.
    if (!wstate->hasBackpressure && wstate->buffered.size() >= upperThreshold)
    {
        backpressureController.applyPressure();
        NES_DEBUG("Backpressure acquired: {} buffered (upper threshold: {})", wstate->buffered.size(), upperThreshold);
        wstate->hasBackpressure = true;
    }

    /// Ensure there is always one pending buffer being retried to avoid deadlocks.
    if (wstate->pendingSequenceNumber == INVALID<SequenceNumber>)
    {
        auto pending = std::move(wstate->buffered.front());
        wstate->buffered.pop_front();
        wstate->pendingSequenceNumber = pending.getSequenceNumber();
        wstate->pendingChunkNumber = pending.getChunkNumber();
        return pending;
    }

    return {};
}

/// Called on a successful send of a buffer to the network channel.
/// Clears the pending buffer and releases backpressure when the buffer count drops to the lower hysteresis threshold.
/// Returns the next buffered tuple to send, if any.
std::optional<TupleBuffer> BackpressureHandler::onSuccess(BackpressureController& backpressureController)
{
    const auto state = stateLock.wlock();
    state->pendingSequenceNumber = INVALID<SequenceNumber>;
    state->pendingChunkNumber = INVALID<ChunkNumber>;

    /// Release backpressure when the buffer count drops to the lower hysteresis threshold.
    if (state->hasBackpressure && state->buffered.size() <= lowerThreshold)
    {
        backpressureController.releasePressure();
        NES_DEBUG("Backpressure released: {} buffered (lower threshold: {})", state->buffered.size(), lowerThreshold);
        state->hasBackpressure = false;
    }

    if (not state->buffered.empty())
    {
        auto nextBuffer = std::move(state->buffered.front());
        state->buffered.pop_front();
        return {nextBuffer};
    }
    return {};
}

bool BackpressureHandler::empty() const
{
    return stateLock.rlock()->buffered.empty();
}

NetworkSink::NetworkSink(
    BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor, std::optional<std::unique_ptr<Encoder>> encoder)
    : Sink(std::move(backpressureController))
    , tupleSize(sinkDescriptor.getSchema()->getSizeOfSchemaInBytes())
    , backpressureHandler(
          sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::BACKPRESSURE_UPPER_THRESHOLD),
          sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::BACKPRESSURE_LOWER_THRESHOLD))
    , channelId(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::CHANNEL))
    , connectionAddr(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::DATA_ENDPOINT))
    , thisConnection(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::BIND))
    , senderQueueSize(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::SENDER_QUEUE_SIZE))
    , maxPendingAcks(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::MAX_PENDING_ACKS))
    , encoder(std::move(encoder))
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
    NES_DEBUG("Sender channel registered: {}", channelId);
}

void NetworkSink::stop(PipelineExecutionContext& pec)
{
    PRECONDITION(channel, "Sender channel is not initialized");
    if (!closed)
    {
        INVARIANT(backpressureHandler.empty(), "BackpressureHandler is not empty");

        /// Check if the sender network service has pending buffers to send
        /// If yes, keep the pipeline alive by emitting an empty buffer
        if (!flush_sender_channel(*this->channel.value()))
        {
            pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
            return;
        }
    }

    NES_DEBUG("Closing Sender channel {}", channelId);
    close_sender_channel(*std::move(this->channel));
    NES_DEBUG("Sender channel {} closed", channelId);
}

void NetworkSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec)
{
    PRECONDITION(channel, "Sender channel is not initialized");
    PRECONDITION(inputBuffer, "Invalid input buffer in NetworkSink.");

    if (closed)
    {
        NES_WARNING("Sink is closed dropping buffer: {}-{}", inputBuffer.getSequenceNumber(), inputBuffer.getChunkNumber());
        return;
    }

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
            .last_chunk = currentBuffer->isLastChunk(),
            .source_insertion_ts = currentBuffer->getSourceCreationTimestampInMS().getRawValue()};

        SendResult sendResult;
        if (encoder)
        {
            /// If an encoder is provided, both the child buffers and the buffer itself may be encoded before transmission.
            /// This functionality is meant for compression codecs that might reduce the overall size of the transmitted data.
            /// Since we mainly care about size advantage, we will only send the encoded result if it is smaller than the original buffer.
            std::vector<rust::Slice<const uint8_t>> children;
            /// States if the ith child is encoded
            std::vector<uint8_t> encodedChildren{};
            std::vector<uint64_t> childBufferSizes{};
            children.reserve(currentBuffer->getNumberOfChildBuffers());
            encodedChildren.reserve(currentBuffer->getNumberOfChildBuffers());
            childBufferSizes.reserve(currentBuffer->getNumberOfChildBuffers());
            for (size_t childIdx = 0; childIdx < currentBuffer->getNumberOfChildBuffers(); ++childIdx)
            {
                /// Encode the child
                auto childBuffer = currentBuffer->loadChildBuffer(VariableSizedAccess::Index(childIdx));
                /// In TupleBufferRef.cpp, copyVarSizedAndIncrementMetaData, the number of tuples of a child buffer corresponds to the written bytes.
                /// We can assume that each buffer in end-to-end queries passes through at least one emit, as we need to enter at least one pipeline for the initial input formatting,
                /// thus we can always read out the amount of written bytes by using the tuple count.
                /// We can use this information to only transmit actually relevant data and then set the number of tuples (bytes) correctly again in NetworkBindings.cpp
                const std::span childMemory(childBuffer.getAvailableMemoryArea<>().data(), childBuffer.getNumberOfTuples());
                std::vector<char> encodedChild{};
                const auto encodingResult = encoder.value()->encodeBuffer(childMemory, encodedChild);
                if (encodingResult.status == Encoder::EncodeStatusType::ENCODING_ERROR
                    || encodingResult.compressedSize >= childBuffer.getNumberOfTuples())
                {
                    /// Send the unencoded child
                    encodedChildren.emplace_back(0);
                    std::span childAsInt8(childBuffer.getAvailableMemoryArea<const uint8_t>().data(), childBuffer.getNumberOfTuples());
                    children.emplace_back(childAsInt8);
                }
                else
                {
                    encodedChildren.emplace_back(1);
                    /// Copy the vector bytes into the child buffer. We need to do that because the vector contents need to persist until we call send_buffer
                    /// Otherwise, once the scope of the current for iteration is exited, the vector seizes to exist.
                    std::memcpy(childBuffer.getAvailableMemoryArea<>().data(), encodedChild.data(), encodingResult.compressedSize);
                    /// The span of the childbuffer that we transmit only holds the encoded bytes
                    const std::span encodedChildSpan(
                        childBuffer.getAvailableMemoryArea<const uint8_t>().data(), encodingResult.compressedSize);
                    children.emplace_back(encodedChildSpan);
                }
                childBufferSizes.emplace_back(childBuffer.getNumberOfTuples());
            }
            const std::span usedBufferMemory(
                currentBuffer->getAvailableMemoryArea<>().data(), currentBuffer->getNumberOfTuples() * tupleSize);
            std::vector<char> encodedBuffer{};
            /// Encode the buffer memory
            const auto encodingResult = encoder.value()->encodeBuffer(usedBufferMemory, encodedBuffer);
            if (encodingResult.status == Encoder::EncodeStatusType::ENCODING_ERROR
                || encodingResult.compressedSize >= usedBufferMemory.size())
            {
                /// Encoded buffer is not smaller than the unencoded buffer. Default to sending the usedBufferMemory
                std::span usedBufferMemoryAsInt8(reinterpret_cast<const uint8_t*>(usedBufferMemory.data()), usedBufferMemory.size());
                sendResult = send_buffer(
                    *channel.value(),
                    metadata,
                    false,
                    rust::Slice<const uint8_t>(encodedChildren.data(), encodedChildren.size()),
                    rust::Slice<const uint64_t>(childBufferSizes.data(), childBufferSizes.size()),
                    rust::Slice(usedBufferMemoryAsInt8),
                    rust::Slice<const rust::Slice<const uint8_t>>(children));
            }
            else
            {
                /// Convert the encoded buffer vector span and send it
                const std::span vectorAsSpan(reinterpret_cast<const uint8_t*>(encodedBuffer.data()), encodingResult.compressedSize);
                sendResult = send_buffer(
                    *channel.value(),
                    metadata,
                    true,
                    rust::Slice<const uint8_t>(encodedChildren.data(), encodedChildren.size()),
                    rust::Slice<const uint64_t>(childBufferSizes.data(), childBufferSizes.size()),
                    rust::Slice(vectorAsSpan),
                    rust::Slice<const rust::Slice<const uint8_t>>(children));
            }
        }
        else
        {
            /// Set child buffers
            std::vector<rust::Slice<const uint8_t>> children;
            children.reserve(currentBuffer->getNumberOfChildBuffers());
            for (size_t childIdx = 0; childIdx < currentBuffer->getNumberOfChildBuffers(); ++childIdx)
            {
                auto childBuffer = currentBuffer->loadChildBuffer(VariableSizedAccess::Index(childIdx));
                auto childMemory = childBuffer.getAvailableMemoryArea<const uint8_t>();
                children.emplace_back(childMemory);
            }

            std::span usedBufferMemory(
                currentBuffer->getAvailableMemoryArea<const uint8_t>().data(), currentBuffer->getNumberOfTuples() * tupleSize);
            /// Set data and send over the network
            /// Since not any children were encoded, we create a vector purely out of 0's
            const std::vector<uint8_t> encodedChildren(inputBuffer.getNumberOfChildBuffers(), 0);
            /// We do not need the childBufferSizes for non-encoded children but need to pass them anyway. However, the vector does not hold the actual
            /// child buffer sizes here.
            const std::vector<uint64_t> childBufferSizes(inputBuffer.getNumberOfChildBuffers(), 0);
            sendResult = send_buffer(
                *channel.value(),
                metadata,
                false,
                rust::Slice(encodedChildren.data(), encodedChildren.size()),
                rust::Slice<const uint64_t>(childBufferSizes.data(), childBufferSizes.size()),
                rust::Slice(usedBufferMemory),
                rust::Slice<const rust::Slice<const uint8_t>>(children));
        }

        switch (sendResult)
        {
            case SendResult::Closed: {
                /// Future buffers are voided.
                this->closed = true;
                [[maybe_unused]] auto droppedBuffer = backpressureHandler.onFull(*currentBuffer, backpressureController);
                /// Currently there is no way to propagate a query stop without a failure from a sink.
                /// There is no operator that propagates a query stop in upstream direction, so receiving a query stop
                /// from the downstream operator is unexpected, thus failing the query is reasonable.
                throw CannotOpenSink("NetworkSink was closed by other side");
            }
            case SendResult::Ok: {
                NES_TRACE("Sending buffer {}", currentBuffer->getSequenceNumber());
                /// Sent a buffer, check the backpressure handler to send another one
                currentBuffer = backpressureHandler.onSuccess(backpressureController);
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

DescriptorConfig::Config NetworkSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersNetworkSink>(std::move(config), name());
}

SinkValidationRegistryReturnType RegisterNetworkSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return NetworkSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterNetworkSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<NetworkSink>(
        std::move(sinkRegistryArguments.backpressureController),
        sinkRegistryArguments.sinkDescriptor,
        std::move(sinkRegistryArguments.encoder));
}

}
