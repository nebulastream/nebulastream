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

#include <TCPSink.hpp>

#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <TCPChannel.hpp>

namespace NES
{

TCPSink::TCPSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , channel(
          sinkDescriptor.getFromConfig(ConfigParametersTCPSink::HOST),
          sinkDescriptor.getFromConfig(ConfigParametersTCPSink::PORT),
          std::chrono::milliseconds{sinkDescriptor.getFromConfig(ConfigParametersTCPSink::CONNECT_TIMEOUT_MS)},
          std::chrono::milliseconds{sinkDescriptor.getFromConfig(ConfigParametersTCPSink::CLOSE_TIMEOUT_MS)},
          sinkDescriptor.getFromConfig(ConfigParametersTCPSink::MAX_QUEUED_BUFFERS))
    , backpressureHandler(
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_UPPER_THRESHOLD),
          sinkDescriptor.getFromConfig(SinkDescriptor::BACKPRESSURE_LOWER_THRESHOLD))
{
}

void TCPSink::start(PipelineExecutionContext&)
{
    channel.start();
}

void TCPSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pec)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in TCPSink.");

    auto currentBuffer = std::optional{inputTupleBuffer};
    while (currentBuffer)
    {
        switch (channel.trySend(*currentBuffer))
        {
            case TCPChannel::SendResult::Accepted: {
                currentBuffer = backpressureHandler.onSuccess(backpressureController);
                continue;
            }
            case TCPChannel::SendResult::Full: {
                if (const auto emit = backpressureHandler.onFull(*currentBuffer, backpressureController))
                {
                    pec.repeatTask(*emit, BACKPRESSURE_RETRY_INTERVAL);
                }
                return;
            }
        }
    }
}

void TCPSink::stop(PipelineExecutionContext& pec)
{
    INVARIANT(backpressureHandler.empty(), "BackpressureHandler is not empty");
    if (!channel.tryClose())
    {
        pec.repeatTask({}, BACKPRESSURE_RETRY_INTERVAL);
        return;
    }
    NES_INFO("TCP Sink completed.");
}

std::ostream& TCPSink::toString(std::ostream& os) const
{
    return os << "TCPSink(socketHost: " << channel.getHost() << ", socketPort: " << channel.getPort() << ")";
}

DescriptorConfig::Config TCPSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTCPSink>(std::move(config), NAME);
}

}
