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

#include "NetworkSink.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/Synchronized.h>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <network/lib.h>
#include <rust/cxx.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

std::optional<TupleBuffer> BackpressureHandler::onFull(TupleBuffer buffer, BackpressureController& backpressureController)
{
    auto rstate = stateLock.ulock();
    if (rstate->hasBackpressure)
    {
        /// Backpressure is already signaled. We want to ensure that at least one TupleBuffer is floating around the TaskQueue,
        /// so we can peridically test if we can send data again.
        /// Otherwise, it might be possible that nothing triggers execution of this pipeline again, resulting in a deadlock
        if (buffer.getSequenceNumber() == rstate->pendingSequenceNumber && buffer.getChunkNumber() == rstate->pendingChunkNumber)
        {
            /// We dedicate one seq/chunk number pair as the pending tuple buffer. If this is the pending tuple buffer we emit it again
            return buffer;
        }

        const auto wstate = rstate.moveFromUpgradeToWrite();
        wstate->buffered.emplace_back(std::move(buffer));
        return {};
    }

    /// Apply backpressure now on the backpressureController, leading to blocked ingestion threads until pressure is released again onSuccess.
    const auto wstate = rstate.moveFromUpgradeToWrite();
    backpressureController.applyPressure();
    wstate->hasBackpressure = true;
    wstate->pendingSequenceNumber = buffer.getSequenceNumber();
    wstate->pendingChunkNumber = buffer.getChunkNumber();
    return buffer;
}

/// Called on a successful send of a buffer to the network channel.
/// 1. If we currently have backpressure, release pressure on the backpressureController, causing ingestion to proceed.
/// 2. In case of buffers remaining in the state, pop the oldest from the deque and return to try to send. Otherwise, return an empty optional.
std::optional<TupleBuffer> BackpressureHandler::onSuccess(BackpressureController& backpressureController)
{
    const auto state = stateLock.wlock();
    if (state->hasBackpressure)
    {
        backpressureController.releasePressure();
        state->hasBackpressure = false;
        state->pendingChunkNumber = INVALID<ChunkNumber>;
        state->pendingSequenceNumber = INVALID<SequenceNumber>;
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

NetworkSink::NetworkSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , tupleSize(sinkDescriptor.getSchema()->getSizeOfSchemaInBytes())
    , channelId(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::CHANNEL))
    , connectionAddr(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::CONNECTION))
    , thisConnection(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::BIND))
{
}

void NetworkSink::start(PipelineExecutionContext&)
{
    this->server = sender_instance(thisConnection);
    this->channel = register_sender_channel(**server, connectionAddr, rust::String(channelId));
    NES_DEBUG("Sender channel registered: {}", channelId);
}

void NetworkSink::stop(PipelineExecutionContext& pec)
{
    if (!closed)
    {
        INVARIANT(backpressureHandler.empty(), "BackpressureHandler is not empty");

        /// Check if the sender network service has pending buffers to send
        /// If yes, keep the pipeline alive by emitting an empty buffer
        if (sender_writes_pending(**this->channel))
        {
            pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
            return;
        }
    }

    close_sender_channel(*std::move(this->channel));
    NES_DEBUG("Sender channel {} closed", channelId);
}

void NetworkSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec)
{
    if (closed)
    {
        return;
    }

    PRECONDITION(inputBuffer, "Invalid input buffer in NetworkSink.");
    /// Set buffer header
    const SerializedTupleBuffer metadata{
        .sequence_number = inputBuffer.getSequenceNumber().getRawValue(),
        .origin_id = inputBuffer.getOriginId().getRawValue(),
        .chunk_number = inputBuffer.getChunkNumber().getRawValue(),
        .number_of_tuples = inputBuffer.getNumberOfTuples(),
        .watermark = inputBuffer.getWatermark().getRawValue(),
        .last_chunk = inputBuffer.isLastChunk()};

    /// Set child buffers
    std::vector<rust::slice<const uint8_t>> children;
    children.reserve(inputBuffer.getNumberOfChildBuffers());
    for (size_t childIdx = 0; childIdx < inputBuffer.getNumberOfChildBuffers(); ++childIdx)
    {
        auto childBuffer = inputBuffer.loadChildBuffer(VariableSizedAccess::Index(childIdx));
        children.emplace_back(childBuffer.getMemArea<uint8_t>(), childBuffer.getBufferSize());
    }

    /// Set data and send over the network
    const auto sendResult = try_send_on_channel(
        **channel,
        metadata,
        rust::slice(inputBuffer.getMemArea<uint8_t>(), inputBuffer.getNumberOfTuples() * tupleSize),
        rust::slice<const rust::slice<const uint8_t>>(children.data(), children.size()));

    switch (sendResult)
    {
        case SendResult::Closed: {
            this->closed = true;
            auto _ = backpressureHandler.onFull(inputBuffer, backpressureController);
            NES_INFO("Network Sink was closed");
            return;
        }
        case SendResult::Ok: {
            /// Sent a buffer, check the backpressure handler to send another one
            if (const auto nextBuffer = backpressureHandler.onSuccess(backpressureController))
            {
                execute(*nextBuffer, pec);
            }
            else
            {
                flush_channel(**channel);
            }
            break;
        }
        case SendResult::Full: {
            if (const auto emit = backpressureHandler.onFull(inputBuffer, backpressureController))
            {
                pec.repeatTask(*emit, BACKPRESSURE_RETRY_INTERVAL);
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
    return std::make_unique<NetworkSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
