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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <TCPChannel.hpp>

namespace NES
{

namespace
{
/// NOLINTBEGIN(cert-err58-cpp,bugprone-throwing-static-initialization)
const ConfigField<std::string> SOCKET_HOST{Identifier::parse("SOCKET_HOST"), "The TCP server host"};

const ConfigField<uint32_t> SOCKET_PORT{
    Identifier::parse("SOCKET_PORT"),
    "The TCP server port, in the range 1-65535",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<int64_t>()).and_then(narrowConfigValue<int64_t, uint32_t, 65535, 1>); }};

const ConfigField<uint32_t> CONNECT_TIMEOUT_MS{
    Identifier::parse("CONNECT_TIMEOUT_MS"),
    "Maximum time establishing or reestablishing a connection, in milliseconds; must be positive",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<int64_t>())
            .and_then(narrowConfigValue<int64_t, uint32_t, std::numeric_limits<uint32_t>::max(), 1>);
    },
    10000};

const ConfigField<uint32_t> CLOSE_TIMEOUT_MS{
    Identifier::parse("CLOSE_TIMEOUT_MS"),
    "Maximum time stopping without write progress, in milliseconds; must be positive",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<int64_t>())
            .and_then(narrowConfigValue<int64_t, uint32_t, std::numeric_limits<uint32_t>::max(), 1>);
    },
    10000};

const ConfigField<size_t> MAX_QUEUED_BUFFERS{
    Identifier::parse("MAX_QUEUED_BUFFERS"),
    "Maximum number of buffers owned by the asynchronous writer; must be positive",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<int64_t>(literal, expectedType<int64_t>())
            .and_then(narrowConfigValue<int64_t, size_t, std::numeric_limits<size_t>::max(), 1>);
    },
    size_t{16}};
/// NOLINTEND(cert-err58-cpp,bugprone-throwing-static-initialization)
}

Schema<QualifiedErasedConfigField, Ordered> TCPSink::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("TCP_SINK"),
        SOCKET_HOST,
        SOCKET_PORT,
        CONNECT_TIMEOUT_MS,
        CLOSE_TIMEOUT_MS,
        MAX_QUEUED_BUFFERS,
        SinkDescriptor::BACKPRESSURE_UPPER_THRESHOLD,
        SinkDescriptor::BACKPRESSURE_LOWER_THRESHOLD);
}

std::expected<TCPSinkConfig, Exception> TCPSinkConfig::fromConfig(const InstantiatedConfig& config)
{
    return TCPSinkConfig{
        .socketHost = config.get(SOCKET_HOST),
        .socketPort = config.get(SOCKET_PORT),
        .connectTimeoutMs = config.get(CONNECT_TIMEOUT_MS),
        .closeTimeoutMs = config.get(CLOSE_TIMEOUT_MS),
        .maxQueuedBuffers = config.get(MAX_QUEUED_BUFFERS)};
}

TCPSink::TCPSink(BackpressureController backpressureController, const TCPSinkConfig& config, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , channel(
          config.socketHost,
          config.socketPort,
          std::chrono::milliseconds{config.connectTimeoutMs},
          std::chrono::milliseconds{config.closeTimeoutMs},
          config.maxQueuedBuffers)
    , backpressureHandler(sinkDescriptor.getBackpressureUpperThreshold(), sinkDescriptor.getBackpressureLowerThreshold())
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
                currentBuffer = backpressureHandler.onSuccess(*currentBuffer, backpressureController);
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

}
