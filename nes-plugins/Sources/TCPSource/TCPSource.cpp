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

#include <cerrno> /// For socket error
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <sys/select.h>

#include <cstdio>
#include <fcntl.h>
#include <netdb.h>
#include <strings.h> /// For strcasecmp
#include <unistd.h> /// For read
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <asm-generic/socket.h>
#include <bits/types/struct_timeval.h>
#include <cpptrace/from_current.hpp>
#include <sys/socket.h> /// For socket functions
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <TCPDataServer.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<std::optional<std::string>> SOCKET_HOST{
    Identifier::parse("SOCKET_HOST"),
    "The host from which to read.",
    [](const ConfigLiteral& literal) -> std::expected<std::optional<std::string>, Exception>
    {
        if (std::holds_alternative<std::monostate>(literal))
        {
            return std::nullopt;
        }
        return NES::tryGetOr<std::string>(literal, expectedType<std::string>());
    }};

const ConfigField<std::optional<uint32_t>> SOCKET_PORT{
    Identifier::parse("SOCKET_PORT"),
    "The port of the host from which to read, any number from 0 to including 65535",
    [](const ConfigLiteral& literal) -> std::expected<std::optional<uint32_t>, Exception>
    {
        if (std::holds_alternative<std::monostate>(literal))
        {
            return std::nullopt;
        }
        return NES::tryGetOr<int64_t>(literal, expectedType<int64_t>())
            .and_then(narrowConfigValue<int64_t, uint32_t, 65535>)
            .transform([](const uint32_t value) { return std::optional{value}; });
    }};

const ConfigField<int32_t> SOCKET_DOMAIN{
    Identifier::parse("SOCKET_DOMAIN"),
    "AF_INET to use IPv4 or AF_INET6 for IPv6 respectively.",
    [](const ConfigLiteral& literal)
    {
        return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](const std::string& value) -> std::expected<int32_t, Exception>
                {
                    if (strcasecmp(value.c_str(), "AF_INET") == 0)
                    {
                        return AF_INET;
                    }
                    if (strcasecmp(value.c_str(), "AF_INET6") == 0)
                    {
                        return AF_INET6;
                    }
                    return std::unexpected{
                        InvalidConfigParameter("TCPSource: Domain value is: {}, but the domain value must be AF_INET or AF_INET6", value)};
                });
    },
    [] { return AF_INET; },
    "AF_INET"};

const ConfigField<int32_t> SOCKET_TYPE{
    Identifier::parse("SOCKET_TYPE"),
    "The socket type, can be SOCK_STREAM, SOCK_DGRAM, SOCK_SEQPACKET, SOCK_RAW or SOCK_RDM.",
    [](const ConfigLiteral& literal)
    {
        return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(Identifier::tryParse)
            .and_then(
                [](const Identifier& socketType) -> std::expected<int32_t, Exception>
                {
                    if (socketType == Identifier::parse("SOCK_STREAM"))
                    {
                        return SOCK_STREAM;
                    }
                    if (socketType == Identifier::parse("SOCK_DGRAM"))
                    {
                        return SOCK_DGRAM;
                    }
                    if (socketType == Identifier::parse("SOCK_SEQPACKET"))
                    {
                        return SOCK_SEQPACKET;
                    }
                    if (socketType == Identifier::parse("SOCK_RAW"))
                    {
                        return SOCK_RAW;
                    }
                    if (socketType == Identifier::parse("SOCK_RDM"))
                    {
                        return SOCK_RDM;
                    }
                    return std::unexpected{InvalidConfigParameter(
                        "TCPSource: Socket type is: {}, but the socket type must be SOCK_STREAM, SOCK_DGRAM, SOCK_SEQPACKET, "
                        "SOCK_RAW, or SOCK_RDM",
                        socketType)};
                });
    },
    SOCK_STREAM,
    "SOCK_STREAM"};

const ConfigField<float> FLUSH_INTERVAL_MS{
    Identifier::parse("FLUSH_INTERVAL_MS"),
    "A float, where a value greater than 0 indicates how many milliseconds to wait before emitting an incomplete buffer.",
    [](const ConfigLiteral& literal)
    { return NES::tryGetDoubleOrInt(literal, expectedType<float>()).and_then(narrowConfigValue<double, float>); },
    0.0F};

const ConfigField<uint32_t> CONNECT_TIMEOUT_SECONDS{
    Identifier::parse("CONNECT_TIMEOUT_SECONDS"),
    "How long to wait until timeout in whole seconds.",
    [](const ConfigLiteral& literal)
    { return NES::tryGetOr<int64_t>(literal, expectedType<int64_t>()).and_then(narrowConfigValue<int64_t, uint32_t>); },
    10};

/// Additional safety marker to prevent people from not setting host and port by accident outside of the systests
const ConfigField<bool> OVERWRITEABLE_HOST_AND_PORT{
    Identifier::parse("OVERWRITEABLE_HOST_AND_PORT"), "Internal marker, do NOT overwrite this", false};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> TCPSource::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("TCP_SOURCE"),
        SOCKET_HOST,
        SOCKET_PORT,
        SOCKET_DOMAIN,
        SOCKET_TYPE,
        FLUSH_INTERVAL_MS,
        CONNECT_TIMEOUT_SECONDS,
        OVERWRITEABLE_HOST_AND_PORT);
}

std::expected<TCPSourceConfig, Exception> TCPSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    bool overwriteableHostAndPort = config.get(OVERWRITEABLE_HOST_AND_PORT);
    if (!overwriteableHostAndPort)
    {
        /// HOST and PORT are optional config fields so that the systests can set them, but at the end of the day they still need to be set.
        if (!config.get(SOCKET_HOST).has_value())
        {
            return std::unexpected{InvalidConfigParameter("TCPSource: Missing required parameter: SOCKET_HOST")};
        }
        if (!config.get(SOCKET_PORT).has_value())
        {
            return std::unexpected{InvalidConfigParameter("TCPSource: Missing required parameter: SOCKET_PORT")};
        }
    }
    else
    {
        if (config.get(SOCKET_HOST).has_value() and config.get(SOCKET_PORT).has_value())
        {
            overwriteableHostAndPort = false;
        }
        if (config.get(SOCKET_HOST).has_value() != config.get(SOCKET_PORT).has_value())
        {
            return std::unexpected{InvalidConfigParameter("TCPSource: HOST and PORT must be set together or left out in systests")};
        }
    }

    /// In the overwriteable (systest) case host/port may be absent; the systest data adaptors
    /// (provideInlineData/provideFileData) fill in the mock server's host and port later.
    return TCPSourceConfig{
        .socketDestination = overwriteableHostAndPort
            ? std::optional<SocketDestination>{}
            : SocketDestination{.socketHost = config.get(SOCKET_HOST).value(), .socketPort = config.get(SOCKET_PORT).value()},
        .socketDomain = config.get(SOCKET_DOMAIN),
        .socketType = config.get(SOCKET_TYPE),
        .flushIntervalInMs = config.get(FLUSH_INTERVAL_MS),
        .connectTimeoutSeconds = config.get(CONNECT_TIMEOUT_SECONDS)};
}

TCPSource::TCPSource(const TCPSourceConfig& config)
    : errBuffer{}
    , socketHost(config.socketDestination.value().socketHost)
    , socketPort(std::to_string(config.socketDestination.value().socketPort))
    , socketType(config.socketType)
    , socketDomain(config.socketDomain)
    , flushIntervalInMs(config.flushIntervalInMs)
    , connectionTimeout(config.connectTimeoutSeconds)
{
    NES_TRACE("Init TCPSource.");
}

std::ostream& TCPSource::toString(std::ostream& str) const
{
    str << "\nTCPSource(";
    str << "\n  generated buffers: " << this->generatedBuffers;
    str << "\n  connection: " << this->connection;
    str << "\n  timeout: " << connectionTimeout << " seconds";
    str << "\n  socketHost: " << socketHost;
    str << "\n  socketPort: " << socketPort;
    str << "\n  socketType: " << socketType;
    str << "\n  socketDomain: " << socketDomain;
    str << "\n  flushIntervalInMs" << flushIntervalInMs;
    str << ")\n";
    return str;
}

bool TCPSource::tryToConnect(const addrinfo* result, const int flags)
{
    const std::chrono::seconds socketConnectDefaultTimeout{connectionTimeout};

    /// we try each addrinfo until we successfully create a socket
    while (result != nullptr)
    {
        sockfd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);

        if (sockfd != -1)
        {
            break;
        }
        result = result->ai_next;
    }

    /// check if we found a vaild address
    if (result == nullptr)
    {
        NES_ERROR("No valid address found to create socket.");
        return false;
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg, hicpp-signed-bitwise) - POSIX API requires varargs
    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

    /// set timeout for both blocking receive and send calls
    /// if timeout is set to zero, then the operation will never timeout
    /// (https://linux.die.net/man/7/socket)
    /// as a workaround, we implicitly add one microsecond to the timeout
    timeval timeout{.tv_sec = socketConnectDefaultTimeout.count(), .tv_usec = IMPLICIT_TIMEOUT_USEC};
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    connection = connect(sockfd, result->ai_addr, result->ai_addrlen);

    /// if the TCPSource did not establish a connection, try with timeout
    if (connection < 0)
    {
        if (errno != EINPROGRESS)
        {
            close();
            /// if connection was unsuccessful, throw an exception with context using errno
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, strerrorResult);
        }

        /// Set the timeout for the connect attempt
        fd_set fdset;
        timeval timeValue{.tv_sec = socketConnectDefaultTimeout.count(), .tv_usec = IMPLICIT_TIMEOUT_USEC};

        FD_ZERO(&fdset);
        FD_SET(sockfd, &fdset);

        connection = select(sockfd + 1, nullptr, &fdset, nullptr, &timeValue);
        if (connection <= 0)
        {
            /// Timeout or error
            errno = ETIMEDOUT;
            close();
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, strerrorResult);
        }

        /// Check if connect succeeded
        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || (error != 0))
        {
            errno = error;
            close();
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, strerrorResult);
        }
    }
    return true;
}

void TCPSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_TRACE("TCPSource::open: Trying to create socket and connect.");

    addrinfo hints{};
    addrinfo* result = nullptr;

    hints.ai_family = socketDomain;
    hints.ai_socktype = socketType;
    hints.ai_flags = 0; /// use default behavior
    hints.ai_protocol
        = 0; /// specifying 0 in this field indicates that socket addresses with any protocol can be returned by getaddrinfo() ;

    const auto errorCode = getaddrinfo(socketHost.c_str(), socketPort.c_str(), &hints, &result);
    if (errorCode != 0)
    {
        throw CannotOpenSource("Failed getaddrinfo with error: {}", gai_strerror(errorCode));
    }

    /// make sure that result is cleaned up automatically (RAII)
    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> resultGuard(result, freeaddrinfo);

    const int flags = fcntl(sockfd, F_GETFL, 0);

    CPPTRACE_TRY
    {
        tryToConnect(result, flags);
    }
    CPPTRACE_CATCH(...)
    {
        ::close(sockfd); /// close socket to clean up state
        throw wrapExternalException("Could not establich connection!");
    }

    /// Set connection to non-blocking again to enable a timeout in the 'read()' call
    fcntl(sockfd, F_SETFL, flags); /// NOLINT(cppcoreguidelines-pro-type-vararg) - POSIX API requires varargs

    NES_TRACE("TCPSource::open: Connected to server.");
}

Source::FillTupleBufferResult TCPSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    try
    {
        size_t numReceivedBytes = 0;
        while (fillBuffer(tupleBuffer, numReceivedBytes))
        {
            /// Fill the buffer until EoS reached or the number of tuples in the buffer is not equals to 0.
        };
        if (numReceivedBytes == 0)
        {
            return FillTupleBufferResult::eos();
        }
        return FillTupleBufferResult::withBytes(numReceivedBytes);
    }
    catch (const std::exception& e)
    {
        NES_ERROR("Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw;
    }
}

bool TCPSource::fillBuffer(TupleBuffer& tupleBuffer, size_t& numReceivedBytes)
{
    const auto flushIntervalTimerStart = std::chrono::system_clock::now();
    bool flushIntervalPassed = false;
    bool readWasValid = true;

    const size_t rawTBSize = tupleBuffer.getBufferSize();
    while (not flushIntervalPassed and numReceivedBytes < rawTBSize)
    {
        const ssize_t bufferSizeReceived
            = read(sockfd, tupleBuffer.getAvailableMemoryArea().data() + numReceivedBytes, rawTBSize - numReceivedBytes);
        numReceivedBytes += bufferSizeReceived;
        if (bufferSizeReceived == INVALID_RECEIVED_BUFFER_SIZE)
        {
            /// if read method returned -1 an error occurred during read.
            NES_ERROR("An error occurred while reading from socket. Error: {}", strerror(errno));
            readWasValid = false;
            numReceivedBytes = 0;
            break;
        }
        if (bufferSizeReceived == EOF_RECEIVED_BUFFER_SIZE)
        {
            NES_TRACE("No data received from {}:{}.", socketHost, socketPort);
            if (numReceivedBytes == 0)
            {
                NES_INFO("TCP Source detected EoS");
                readWasValid = false;
                break;
            }
        }
        /// If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
        /// and writing data exceeds the user defined limit (bufferFlushIntervalMs).
        /// If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
        if ((flushIntervalInMs > 0
             && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - flushIntervalTimerStart).count()
                 >= flushIntervalInMs))
        {
            NES_DEBUG("Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
    }
    ++generatedBuffers;
    /// Loop while we haven't received any bytes yet and we can still read from the socket.
    return numReceivedBytes == 0 and readWasValid;
}

void TCPSource::close()
{
    NES_DEBUG("Trying to close connection.");
    if (connection >= 0)
    {
        ::close(sockfd);
        NES_TRACE("Connection closed.");
    }
}

InlineDataRegistryReturnType TCPSource::provideInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    std::unordered_map<Identifier, std::string> defaultSourceConfig{{Identifier::parse("flush_interval_ms"), "100"}};
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.merge(defaultSourceConfig);

    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse("SOCKET_PORT")))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse("SOCKET_HOST")))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.tuples));

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        Identifier::parse("SOCKET_PORT"), std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(Identifier::parse("SOCKET_HOST"), "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

FileDataRegistryReturnType TCPSource::provideFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    std::unordered_map<Identifier, std::string> defaultSourceConfig{{Identifier::parse("flush_interval_ms"), "100"}};
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.merge(defaultSourceConfig);

    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse("SOCKET_PORT")))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse("SOCKET_HOST")))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }


    auto mockTCPServer = std::make_unique<TCPDataServer>(systestAdaptorArguments.testFilePath);

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        Identifier::parse("SOCKET_PORT"), std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(Identifier::parse("SOCKET_HOST"), "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}
}
