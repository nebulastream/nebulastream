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

#include <HandoffSink.hpp>

#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <Async/HandoffChannel.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

HandoffSink::HandoffSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , channelId(sinkDescriptor.getFromConfig(ConfigParametersHandoffSink::CHANNEL))
    , channelCapacity(sinkDescriptor.getFromConfig(ConfigParametersHandoffSink::CHANNEL_CAPACITY))
    , backpressureHandler(
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_UPPER_THRESHOLD),
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_LOWER_THRESHOLD))
{
}

void HandoffSink::start(PipelineExecutionContext&)
{
    /// Either half may start first — the two plans are deployed independently — so whoever
    /// gets here first creates the channel.
    channel = HandoffChannelRegistry::getOrCreate(channelId, channelCapacity);
    NES_DEBUG("Handoff sink attached to channel {}", channelId);
}

void HandoffSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in HandoffSink.");
    INVARIANT(channel != nullptr, "HandoffSink::execute was called before start()");

    auto currentBuffer = std::optional{inputTupleBuffer};
    while (currentBuffer)
    {
        if (channel->tryPush(*currentBuffer))
        {
            /// Accepted: take whatever the handler has queued up and try that too.
            currentBuffer = backpressureHandler.onSuccess(backpressureController);
            continue;
        }

        /// Channel full. Stash the buffer and let the engine come back to us; this is the
        /// only thing that throttles the upstream sources.
        if (const auto retry = backpressureHandler.onFull(*currentBuffer, backpressureController))
        {
            pipelineExecutionContext.repeatTask(*retry, RETRY_INTERVAL);
        }
        return;
    }
}

void HandoffSink::stop(PipelineExecutionContext& pipelineExecutionContext)
{
    INVARIANT(channel != nullptr, "HandoffSink::stop was called before start()");

    /// Hand over whatever is still stashed before closing, otherwise those records are lost.
    /// Not finishing the stop is expressed the way NetworkSink expresses it: repeat the task.
    while (auto pending = backpressureHandler.onSuccess(backpressureController))
    {
        if (!channel->tryPush(*pending))
        {
            std::ignore = backpressureHandler.onFull(*pending, backpressureController);
            pipelineExecutionContext.repeatTask({}, RETRY_INTERVAL);
            return;
        }
    }

    /// Drain first, then close: buffers already in the channel are still handed out, and
    /// only when it runs empty does the consumer see the end of the stream.
    channel->close();
    NES_INFO("Handoff sink closed channel {}", channelId);
}

DescriptorConfig::Config HandoffSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersHandoffSink>(std::move(config), std::string{NAME});
}

std::ostream& HandoffSink::toString(std::ostream& os) const
{
    return os << "HandoffSink(channel: " << channelId << ")";
}

}
