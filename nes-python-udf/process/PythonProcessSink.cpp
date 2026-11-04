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

#include <PythonProcessSink.hpp>

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <utility>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <PythonProcessChannel.hpp>

namespace NES
{

namespace
{
/// NOLINTNEXTLINE(bugprone-throwing-static-initialization,cert-err58-cpp)
const ConfigField<std::string> CHANNEL{
    Identifier::parse("CHANNEL"),
    "Id of the channel the PythonProcess source of the same UDF operator takes the buffers from",
    [](const ConfigLiteral& literal) { return NES::tryGetOr<std::string>(literal, expectedType<std::string>()); }};
}

Schema<QualifiedErasedConfigField, Ordered> PythonProcessSink::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("PYTHON_PROCESS_SINK"), CHANNEL);
}

std::expected<PythonProcessSinkConfig, Exception> PythonProcessSinkConfig::fromConfig(const InstantiatedConfig& config)
{
    return PythonProcessSinkConfig{.channel = config.get(CHANNEL)};
}

PythonProcessSink::PythonProcessSink(BackpressureController backpressureController, const PythonProcessSinkConfig& config)
    : Sink(std::move(backpressureController)), channel(config.channel, PythonProcessChannel::Role::SINK)
{
}

/// Only a graceful stop gets here. A hard stop skips it, and the lease removes the channel if no source ever attached.
void PythonProcessSink::stop(PipelineExecutionContext&)
{
    channel->close();
}

void PythonProcessSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
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
            case PythonProcessChannel::PushResult::PUSHED:
                currentBuffer = backpressureHandler.onSuccess(*currentBuffer, backpressureController);
                break;
            case PythonProcessChannel::PushResult::FULL:
                if (const auto retry = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pipelineExecutionContext.repeatTask(*retry, RETRY_INTERVAL);
                }
                return;
            case PythonProcessChannel::PushResult::CONSUMER_GONE:
                /// The query is stopping or has failed. Retrying would keep this half of the query running forever.
                consumerGone = true;
                dropRemaining(*currentBuffer);
                return;
        }
    }
}

void PythonProcessSink::dropRemaining(const TupleBuffer& buffer)
{
    /// Reports the buffer as taken, and every buffer the handler hands on after it.
    for (auto next = std::optional(buffer); next; next = backpressureHandler.onSuccess(*next, backpressureController))
    {
    }
}

}
