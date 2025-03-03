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
#include <network/lib.h>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <magic_enum.hpp>
#include "NetworkSinkValidation.hpp"
#include "rust/cxx.h"

namespace NES::Sinks
{

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
        pec.emitBuffer({}, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::RETRY);
        return;
    }

    NES_INFO("Closing Network Sink")
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
        case SendResult::Full:
            if (valve.apply_pressure())
            {
                pec.emitBuffer(inputBuffer, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::RETRY);
            }
            else
            {
                buffers.wlock()->push_back(inputBuffer);
            }
            return;
        case SendResult::Error:
            throw CannotOpenSink("Sink Failed");
        case SendResult::Ok:
            flush_channel(**channel);
            NES_INFO("Send {} was successful", inputBuffer.getSequenceNumber());
    }

    /// check pending buffers
    auto readBuffers = buffers.ulock();
    if (readBuffers->empty())
    {
        valve.release_pressure();
        return;
    }

    auto writeBuffers = readBuffers.moveFromUpgradeToWrite();
    if (writeBuffers->empty())
    {
        valve.release_pressure();
        return;
    }

    auto newBuffer = std::move(writeBuffers->back());
    writeBuffers->pop_back();
    writeBuffers.unlock();

    return NetworkSink::execute(newBuffer, pec);
}

std::ostream& NetworkSink::toString(std::ostream& str) const
{
    return str << fmt::format("NETWORK_SINK({})", channelIdentifier);
}

std::unique_ptr<SinkRegistryReturnType> SinkGeneratedRegistrar::RegisterNetworkSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<NetworkSink>(std::move(sinkRegistryArguments.valve), sinkRegistryArguments.sinkDescriptor);
}

}
