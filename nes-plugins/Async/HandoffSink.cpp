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
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
HandoffSink::HandoffSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController)), channelId(sinkDescriptor.getFromConfig(ConfigParametersHandoff::CHANNEL))
{
}

void HandoffSink::start(PipelineExecutionContext&)
{
    channel = HandoffChannel::get(channelId);
}

void HandoffSink::stop(PipelineExecutionContext&)
{
    /// Stop also runs for a sink whose start failed.
    if (channel)
    {
        channel->close();
        channel.reset();
    }
}

void HandoffSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    auto currentBuffer = std::optional(inputTupleBuffer);
    while (currentBuffer)
    {
        if (!channel->tryPush(*currentBuffer))
        {
            /// The Async source only takes the next buffer once the calls of its current one finished, so a full
            /// channel means the calls fall behind: hold the producer's sources back until it drains.
            if (const auto retry = backpressureHandler.onFull(*currentBuffer, backpressureController))
            {
                pipelineExecutionContext.repeatTask(*retry, RETRY_INTERVAL);
            }
            return;
        }
        currentBuffer = backpressureHandler.onSuccess(*currentBuffer, backpressureController);
    }
}

DescriptorConfig::Config HandoffSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersHandoff>(std::move(config), NAME);
}

}
