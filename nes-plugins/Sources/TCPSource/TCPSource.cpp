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

#include <TCPSource.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <TCPDataServer.hpp>
#include <TCPSocket.hpp>

namespace NES
{

TCPSource::TCPSource(const SourceDescriptor& sourceDescriptor)
    : socketHost(sourceDescriptor.getFromConfig(ConfigParametersTCP::HOST))
    , socketPort(sourceDescriptor.getFromConfig(ConfigParametersTCP::PORT))
    , flushInterval(static_cast<uint32_t>(sourceDescriptor.getFromConfig(ConfigParametersTCP::FLUSH_INTERVAL_MS)))
    , connectionTimeout(sourceDescriptor.getFromConfig(ConfigParametersTCP::CONNECT_TIMEOUT_MS))
    , autoReconnect(sourceDescriptor.getFromConfig(ConfigParametersTCP::AUTO_RECONNECT))
{
    NES_TRACE("Init TCPSource.");
}

std::ostream& TCPSource::toString(std::ostream& str) const
{
    fmt::print(
        str,
        "\nTCPSource("
        "\n  generatedBuffers: {}"
        "\n  connectionTimeout: {} (0 = no timeout)"
        "\n  socketHost: {}"
        "\n  socketPort: {}"
        "\n  flushInterval: {}"
        "\n  autoReconnect: {}"
        ")\n",
        generatedBuffers,
        connectionTimeout,
        socketHost,
        socketPort,
        flushInterval,
        autoReconnect);
    return str;
}

void TCPSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_TRACE("TCPSource::open: Will connect on first fillTupleBuffer call.");
}

Source::FillTupleBufferResult TCPSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    while (true)
    {
        auto result = std::visit(
            Overloaded{
                [&](Connecting& connecting) -> std::optional<FillTupleBufferResult>
                {
                    constexpr auto initialBackoff = std::chrono::milliseconds{100};
                    constexpr auto maxBackoff = std::chrono::milliseconds{5000};

                    /// Wait for backoff period before attempting (0 on first attempt)
                    if (connecting.backoff > std::chrono::milliseconds{0})
                    {
                        NES_TRACE("TCPSource: waiting {} before reconnect", connecting.backoff);
                        if (std::holds_alternative<Stopped>(cancellableSleep(connecting.backoff, stopToken)))
                        {
                            return FillTupleBufferResult::eos();
                        }
                    }

                    NES_TRACE("TCPSource: attempting to connect");
                    auto connectResult = TCPSocket::connect(socketHost, socketPort, stopToken, connectionTimeout);

                    return std::visit(
                        Overloaded{
                            [&](TCPSocket& socket) -> std::optional<FillTupleBufferResult>
                            {
                                NES_DEBUG("TCPSource: connected");
                                state = Receiving{.socket = std::move(socket)};
                                return std::nullopt;
                            },
                            [](Stopped&) -> std::optional<FillTupleBufferResult> { return FillTupleBufferResult::eos(); },
                            [&](TCPSocket::Timeout&) -> std::optional<FillTupleBufferResult>
                            {
                                NES_TRACE("TCPSource: connect timed out");
                                connecting.backoff = std::min(std::max(connecting.backoff * 2, initialBackoff), maxBackoff);
                                return std::nullopt;
                            },
                            [&](TCPSocket::ConnectError& error) -> std::optional<FillTupleBufferResult>
                            {
                                NES_TRACE("TCPSource: connect failed: {}", error);
                                connecting.backoff = std::min(std::max(connecting.backoff * 2, initialBackoff), maxBackoff);
                                return std::nullopt;
                            },
                        },
                        connectResult);
                },
                [&](Receiving& receiving) -> std::optional<FillTupleBufferResult>
                {
                    auto recvResult = receiving.socket.recvExact(tupleBuffer.getAvailableMemoryArea<uint8_t>(), flushInterval, stopToken);

                    return std::visit(
                        Overloaded{
                            [&](std::span<uint8_t> data) -> std::optional<FillTupleBufferResult>
                            {
                                ++generatedBuffers;
                                return FillTupleBufferResult::withBytes(data.size());
                            },
                            [](TCPSocket::Timeout&) -> std::optional<FillTupleBufferResult> { return std::nullopt; },
                            [](Stopped&) -> std::optional<FillTupleBufferResult> { return FillTupleBufferResult::eos(); },
                            [&](TCPSocket::EoS&) -> std::optional<FillTupleBufferResult>
                            {
                                if (autoReconnect)
                                {
                                    NES_WARNING("TCPSource: disconnected, reconnecting");
                                    state = Connecting{};
                                    return std::nullopt;
                                }
                                state = EoS{};
                                return std::nullopt;
                            },
                            [&](std::string& recvError) -> std::optional<FillTupleBufferResult>
                            {
                                if (autoReconnect)
                                {
                                    NES_WARNING("TCPSource: recv error: {}, reconnecting", recvError);
                                    state = Connecting{};
                                    return std::nullopt;
                                }
                                throw CannotProduceSourceBuffer(recvError);
                            },
                        },
                        recvResult);
                },
                [](EoS&) -> std::optional<FillTupleBufferResult> { return FillTupleBufferResult::eos(); },
            },
            state);

        if (result.has_value())
        {
            return *result;
        }
    }
}

DescriptorConfig::Config TCPSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), name());
}

void TCPSource::close()
{
    state = EoS{};
}

SourceValidationRegistryReturnType RegisterTCPSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return TCPSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterTCPSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<TCPSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterTCPInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    std::unordered_map<std::string, std::string> defaultSourceConfig{{"flush_interval_ms", "100"}};
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.merge(defaultSourceConfig);

    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersTCP::PORT))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersTCP::HOST))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.tuples));

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersTCP::PORT, std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersTCP::HOST, "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterTCPFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    std::unordered_map<std::string, std::string> defaultSourceConfig{{"flush_interval_ms", "100"}};
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.merge(defaultSourceConfig);

    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersTCP::PORT))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersTCP::HOST))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }


    auto mockTCPServer = std::make_unique<TCPDataServer>(systestAdaptorArguments.testFilePath);

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersTCP::PORT, std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersTCP::HOST, "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}
}
