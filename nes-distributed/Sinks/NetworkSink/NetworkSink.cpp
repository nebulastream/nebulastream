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

#include <cstddef>
#include <cstdint>
#include <deque>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <magic_enum/magic_enum.hpp>
#include <network/lib.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES::Sinks
{


std::optional<Memory::TupleBuffer> BackpressureHandler::onFull(Memory::TupleBuffer buffer, Valve& valve)
{
    auto rstate = stateLock.ulock();
    if (rstate->hasBackpressure)
    {
        /// back pressure is already signaled. We want to ensure that at least one TupleBuffer is floating around the TaskQueue,
        /// so we can peridically test if we can send data again.
        if (buffer.getSequenceNumber() == rstate->pendingSequenceNumber && buffer.getChunkNumber() == rstate->pendingChunkNumber)
        {
            /// We dedicate one seq/chunk number pair as the pending tuple buffer. If this is the pending tuple buffer we emit it again
            return buffer;
        }

        auto wstate = rstate.moveFromUpgradeToWrite();
        wstate->buffered.emplace_back(buffer);
        NES_WARNING("Backpressure Level: {}", wstate->buffered.size());
        return {};
    }
    else
    {
        auto wstate = rstate.moveFromUpgradeToWrite();
        NES_WARNING("Applying backpressure");
        valve.apply_pressure();
        wstate->hasBackpressure = true;
        wstate->pendingSequenceNumber = buffer.getSequenceNumber();
        wstate->pendingChunkNumber = buffer.getChunkNumber();
        return buffer;
    }
}

std::optional<Memory::TupleBuffer> BackpressureHandler::onSuccess(Valve& valve)
{
    auto state = stateLock.wlock();
    if (state->hasBackpressure)
    {
        valve.release_pressure();
        state->hasBackpressure = false;
        state->pendingChunkNumber = INVALID<ChunkNumber>;
        state->pendingSequenceNumber = INVALID<SequenceNumber>;
    }

    if (!state->buffered.empty())
    {
        auto nextBuffer = std::move(state->buffered.front());
        state->buffered.pop_front();
        NES_WARNING("Backpressure Level: {}", state->buffered.size());
        return nextBuffer;
    }

    return {};
}

NetworkSink::NetworkSink(Valve valve, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(valve))
    , tupleSize(sinkDescriptor.schema->getSchemaSizeInBytes())
    , channelIdentifier(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::CHANNEL))
    , connectionIdentifier(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::CONNECTION))
{
}

void NetworkSink::start(Runtime::Execution::PipelineExecutionContext&)
{
    this->server = sender_instance();
    this->channel = register_sender_channel(**server, connectionIdentifier, rust::String(channelIdentifier));
}

void NetworkSink::stop(Runtime::Execution::PipelineExecutionContext& pec)
{
    if (sender_writes_pending(**this->channel))
    {
        pec.emitBuffer({}, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::REPEAT);
        return;
    }

    NES_INFO("Closing Network Sink. Number of Buffers transmitted: {}", buffersSend.load());
    close_sender_channel(*std::move(this->channel));
    NES_INFO("Close was successful?");
}

void NetworkSink::execute(const Memory::TupleBuffer& inputBuffer, Runtime::Execution::PipelineExecutionContext& pec)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in NetworkSink.");
    SerializedTupleBuffer meta{
        .sequence_number = inputBuffer.getSequenceNumber().getRawValue(),
        .origin_id = inputBuffer.getOriginId().getRawValue(),
        .chunk_number = inputBuffer.getChunkNumber().getRawValue(),
        .number_of_tuples = inputBuffer.getNumberOfTuples(),
        .watermark = inputBuffer.getWatermark().getRawValue(),
        .last_chunk = inputBuffer.isLastChunk()};

    std::vector<rust::slice<const uint8_t>> children;
    children.reserve(inputBuffer.getNumberOfChildrenBuffer());
    for (size_t childIdx = 0; childIdx < inputBuffer.getNumberOfChildrenBuffer(); ++childIdx)
    {
        auto childBuffer = inputBuffer.loadChildBuffer(childIdx);
        children.emplace_back(childBuffer.getBuffer<uint8_t>(), childBuffer.getBufferSize());
    }
    auto result = send_channel(
        **channel,
        meta,
        rust::slice<const uint8_t>(inputBuffer.getBuffer<uint8_t>(), inputBuffer.getNumberOfTuples() * tupleSize),
        rust::slice<const rust::slice<const uint8_t>>(children.data(), children.size()));

    switch (result)
    {
        case SendResult::Error:
            throw CannotOpenSink("Sink Failed");
        case SendResult::Ok:
            ++buffersSend;
            if (auto nextBuffer = backpressureHandler.onSuccess(valve))
            {
                execute(*nextBuffer, pec);
            }
            else
            {
                flush_channel(**channel);
            }
            return;
        case SendResult::Full:
            if (auto emit = backpressureHandler.onFull(inputBuffer, valve))
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
                pec.emitBuffer(*emit, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::REPEAT);
            }
            return;
    }
}

std::ostream& NetworkSink::toString(std::ostream& str) const
{
    return str << fmt::format("NETWORK_SINK({})", channelIdentifier);
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterNetworkSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<NetworkSink>(std::move(sinkRegistryArguments.valve), sinkRegistryArguments.sinkDescriptor);
}

}
