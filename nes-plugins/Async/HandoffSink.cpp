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
#include <expected>
#include <optional>
#include <utility>
#include <vector>
#include <Identifiers/Identifier.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/HandoffSinkConfig.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
HandoffSink::HandoffSink(BackpressureController backpressureController, const HandoffSinkConfig& config)
    : Sink(std::move(backpressureController))
    , channel(config.channel ? config.channel->takeSender() : throw InvalidConfigParameter("HandoffSink requires a non-null channel"))
{
}

void HandoffSink::start(PipelineExecutionContext&)
{
}

/// Only a graceful stop gets here. A hard stop destroys the sender without closing it and aborts the receiver.
void HandoffSink::stop(PipelineExecutionContext&)
{
    channel.close();
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
        switch (channel.tryPush(*currentBuffer))
        {
            case HandoffChannel::PushResult::PUSHED:
                currentBuffer = backpressureHandler.onSuccess(*currentBuffer, backpressureController);
                break;
            case HandoffChannel::PushResult::FULL:
                /// The calls fall behind: hold the producer's sources back until the channel takes buffers again.
                if (const auto retry = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pipelineExecutionContext.repeatTask(*retry, RETRY_INTERVAL);
                }
                return;
            case HandoffChannel::PushResult::CONSUMER_GONE:
                /// The query is stopping or has failed, and nothing will take a buffer again. Retrying would keep this
                /// half of the query running forever, so the rest is dropped and the producer's sources are released.
                consumerGone = true;
                NES_DEBUG("Async source has stopped; dropping the buffers still to be handed off");
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

Schema<QualifiedErasedConfigField, Ordered> HandoffSink::getConfigSchema()
{
    return Schema<QualifiedErasedConfigField, Ordered>{std::vector<QualifiedErasedConfigField>{}};
}

std::expected<HandoffSinkConfig, Exception> HandoffSinkConfig::fromConfig(const InstantiatedConfig&)
{
    return std::unexpected{InvalidConfigParameter("Handoff sinks require a planner-supplied channel, not literal configuration")};
}

}
