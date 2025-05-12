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
#include "NetworkSinkValidation.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/Synchronized.h>

#include <Identifiers/Identifiers.hpp>
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

namespace NES::Sinks
{

std::optional<Memory::TupleBuffer> BackpressureHandler::onFull(Memory::TupleBuffer buffer, Valve& valve)
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
        NES_WARNING("Backpressure Level: {}", wstate->buffered.size());
        return {};
    }

    /// Apply backpressure now on the valve, leading to blocked ingestion threads until pressure is released again onSuccess.
    const auto wstate = rstate.moveFromUpgradeToWrite();
    NES_WARNING("Applying backpressure");
    valve.applyPressure();
    wstate->hasBackpressure = true;
    wstate->pendingSequenceNumber = buffer.getSequenceNumber();
    wstate->pendingChunkNumber = buffer.getChunkNumber();
    return buffer;
}

/// Called on a successful send of a buffer to the network channel.
/// 1. If we currently have backpressure, release pressure on the valve, causing ingestion to proceed.
/// 2. In case of buffers remaining in the state, pop the oldest from the deque and return to try to send. Otherwise, return an empty optional.
std::optional<Memory::TupleBuffer> BackpressureHandler::onSuccess(Valve& valve)
{
    const auto state = stateLock.wlock();
    if (state->hasBackpressure)
    {
        valve.releasePressure();
        state->hasBackpressure = false;
        state->pendingChunkNumber = INVALID<ChunkNumber>;
        state->pendingSequenceNumber = INVALID<SequenceNumber>;
    }

    if (not state->buffered.empty())
    {
        auto nextBuffer = std::move(state->buffered.front());
        state->buffered.pop_front();
        NES_WARNING("Backpressure Level: {}", state->buffered.size());
        return {nextBuffer};
    }
    return {};
}

NetworkSink::NetworkSink(Valve valve, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(valve))
    , tupleSize(sinkDescriptor.schema.getSizeOfSchemaInBytes())
    , channelId(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::CHANNEL))
    , connectionId(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::CONNECTION))
{
}

void NetworkSink::start(PipelineExecutionContext&)
{
    this->server = sender_instance();
    this->channel = register_sender_channel(**server, connectionId, rust::String(channelId));
}

void NetworkSink::stop(PipelineExecutionContext& pec)
{
    /// Check if the sender network service has pending buffers to send
    /// If yes, keep the pipeline alive by emitting an empty buffer
    if (sender_writes_pending(**this->channel))
    {
        pec.emitBuffer({}, PipelineExecutionContext::ContinuationPolicy::REPEAT);
        return;
    }

    NES_INFO("Closing Network Sink. Number of Buffers transmitted: {}", buffersSent.load());
    close_sender_channel(*std::move(this->channel));
    NES_INFO("Network Channel successfully closed");
}

void NetworkSink::execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext& pec)
{
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
    children.reserve(inputBuffer.getNumberOfChildrenBuffer());
    for (size_t childIdx = 0; childIdx < inputBuffer.getNumberOfChildrenBuffer(); ++childIdx)
    {
        auto childBuffer = inputBuffer.loadChildBuffer(childIdx);
        children.emplace_back(childBuffer.getBuffer<uint8_t>(), childBuffer.getBufferSize());
    }

    /// Set data and send over the network
    const auto sendResult = send_channel(
        **channel,
        metadata,
        rust::slice(inputBuffer.getBuffer<uint8_t>(), inputBuffer.getNumberOfTuples() * tupleSize),
        rust::slice<const rust::slice<const uint8_t>>(children.data(), children.size()));

    switch (sendResult)
    {
        case SendResult::Error:
            throw CannotOpenSink(
                "Sink Failed"); /// This cannot be the case currently, as the sender queue is not closed from anywhere on the cxx side
        case SendResult::Ok: {
            ++buffersSent;
            /// Sent a buffer, check the backpressure handler to send another one
            if (const auto nextBuffer = backpressureHandler.onSuccess(valve))
            {
                execute(*nextBuffer, pec);
            }
            else
            {
                flush_channel(**channel);
            }
        }
        case SendResult::Full: {
            if (const auto emit = backpressureHandler.onFull(inputBuffer, valve))
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
                pec.emitBuffer(*emit, PipelineExecutionContext::ContinuationPolicy::REPEAT);
            }
        }
    }
}

std::ostream& NetworkSink::toString(std::ostream& str) const
{
    return str << fmt::format("NETWORK_SINK({})", channelId);
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterNetworkSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<NetworkSink>(std::move(sinkRegistryArguments.valve), sinkRegistryArguments.sinkDescriptor);
}

}
