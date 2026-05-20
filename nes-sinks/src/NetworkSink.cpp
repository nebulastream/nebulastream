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
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <network/lib.h>
#include <rust/cxx.h>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

NetworkSink::NetworkSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , tupleSize(NES::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptor.getSchema())->getSizeInBytes())
    , backpressureHandler(
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_UPPER_THRESHOLD),
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_LOWER_THRESHOLD))
    , channelId(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::CHANNEL))
    , connectionAddr(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::DATA_ENDPOINT))
    , thisConnection(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::BIND))
    , senderQueueSize(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::SENDER_QUEUE_SIZE))
    , maxPendingAcks(sinkDescriptor.getFromConfig(ConfigParametersNetworkSink::MAX_PENDING_ACKS))
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
            .last_chunk = currentBuffer->isLastChunk()};

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
    return std::make_unique<NetworkSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
