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

#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
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
    , channelId(sinkDescriptor.getFromConfig(ConfigParametersHandoff::CHANNEL))
    , channel(channelId, sinkDescriptor.getFromConfig(ConfigParametersHandoff::CHANNEL_CAPACITY), HandoffChannel::Role::SINK)
{
}

void HandoffSink::start(PipelineExecutionContext&)
{
}

/// Only a graceful stop gets here. A hard stop skips it, and the lease, which goes with the sink, removes the channel if
/// no source ever attached; closing it there would end the source's stream as if the producer had finished.
void HandoffSink::stop(PipelineExecutionContext&)
{
    channel->close();
}

void HandoffSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    if (consumerGone)
    {
        dropRemaining(inputTupleBuffer);
        return;
    }
    auto currentBuffer = std::optional(inputTupleBuffer);
    while (currentBuffer)
    {
        switch (channel->tryPush(*currentBuffer))
        {
            case HandoffChannel::PushResult::PUSHED:
                currentBuffer = backpressureHandler.onSuccess(*currentBuffer, backpressureController);
                break;
            case HandoffChannel::PushResult::FULL:
                /// The calls fall behind, or the Async source has not attached yet: hold the producer's sources back
                /// until the channel takes buffers again.
                if (const auto retry = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pipelineExecutionContext.repeatTask(*retry, RETRY_INTERVAL);
                }
                return;
            case HandoffChannel::PushResult::CONSUMER_GONE:
                /// The query is stopping or has failed, and nothing will take a buffer again. Retrying would keep this
                /// half of the query running forever, so the rest is dropped and the producer's sources are released.
                consumerGone = true;
                NES_DEBUG("Async source of channel {} has stopped; dropping the buffers still to be handed off", channelId);
                dropRemaining(*currentBuffer);
                return;
        }
    }
}

void HandoffSink::dropRemaining(const TupleBuffer& buffer)
{
    /// Reports the buffer as taken, and every buffer the handler hands on after it. Only the handler's pending buffer
    /// advances it; when that one is held by a retry task, the task brings it back here.
    for (auto next = std::optional(buffer); next; next = backpressureHandler.onSuccess(*next, backpressureController))
    {
    }
}

DescriptorConfig::Config HandoffSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersHandoff>(std::move(config), NAME);
}

}
