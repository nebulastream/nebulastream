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
#include <ostream>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <sys/select.h>

#include <cstdio>
#include <fcntl.h>
#include <netdb.h>
#include <strings.h> /// For strcasecmp
#include <unistd.h> /// For read
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
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

/// Config fields of the TCP source, shared by getConfigSchema (declaration) and
/// TCPSourceConfig::fromConfig (typed extraction).
/// NOLINTBEGIN(cert-err58-cpp)
static const ConfigField<std::string> SOCKET_HOST{
    "SOCKET_HOST", [](const ConfigLiteral& literal) { return NES::tryGetOr<std::string>(literal, expectedType<std::string>()); }};

static const ConfigField<uint32_t> SOCKET_PORT{
    "SOCKET_PORT",
    [](const ConfigLiteral& literal)
    { return NES::tryGetOr<int64_t>(literal, expectedType<int64_t>()).and_then(downcastConfigValue<int64_t, uint32_t, 65535>); }};

static const ConfigField<int32_t> SOCKET_DOMAIN{
    "SOCKET_DOMAIN",
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
    [] { return AF_INET; }};

static const ConfigField<int32_t> SOCKET_TYPE{
    "SOCKET_TYPE",
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
    SOCK_STREAM};

static const ConfigField<char> TUPLE_DELIMITER{
    "TUPLE_DELIMITER",
    [](const ConfigLiteral& literal)
    {
        return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](const std::string& value) -> std::expected<char, Exception>
                {
                    if (value.size() == 1)
                    {
                        return value.front();
                    }
                    return std::unexpected{InvalidConfigParameter("Expected a single character, got {}", value)};
                });
    },
    '\n'};

static const ConfigField<float> FLUSH_INTERVAL_MS{
    "FLUSH_INTERVAL_MS",
    [](const ConfigLiteral& literal)
    {
        /// Fractional literals arrive as double, integer ones as int64_t.
        return NES::tryGetOr<double>(literal, expectedType<float>())
            .or_else(
                [&literal](const Exception&)
                {
                    return NES::tryGetOr<int64_t>(literal, expectedType<float>())
                        .transform([](const int64_t value) { return static_cast<double>(value); });
                })
            .and_then(downcastConfigValue<double, float>);
    },
    0.0F};

static const ConfigField<uint32_t> SOCKET_BUFFER_SIZE{
    "SOCKET_BUFFER_SIZE",
    [](const ConfigLiteral& literal)
    { return NES::tryGetOr<int64_t>(literal, expectedType<int64_t>()).and_then(downcastConfigValue<int64_t, uint32_t>); },
    1024};

static const ConfigField<uint32_t> SOCKET_BUFFER_TRANSFER_SIZE{
    "BYTES_SED_FOR_SOCKET_BUFFER_SIZE_TRANSFER",
    [](const ConfigLiteral& literal)
    { return NES::tryGetOr<int64_t>(literal, expectedType<int64_t>()).and_then(downcastConfigValue<int64_t, uint32_t>); },
    0};

static const ConfigField<uint32_t> CONNECT_TIMEOUT_SECONDS{
    "CONNECT_TIMEOUT_SECONDS",
    [](const ConfigLiteral& literal)
    { return NES::tryGetOr<int64_t>(literal, expectedType<int64_t>()).and_then(downcastConfigValue<int64_t, uint32_t>); },
    10};
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
        TUPLE_DELIMITER,
        FLUSH_INTERVAL_MS,
        SOCKET_BUFFER_SIZE,
        SOCKET_BUFFER_TRANSFER_SIZE,
        CONNECT_TIMEOUT_SECONDS);
}

TCPSourceConfig TCPSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    return TCPSourceConfig{
        .socketHost = config.get(SOCKET_HOST),
        .socketPort = config.get(SOCKET_PORT),
        .socketDomain = config.get(SOCKET_DOMAIN),
        .socketType = config.get(SOCKET_TYPE),
        .tupleDelimiter = config.get(TUPLE_DELIMITER),
        .socketBufferSize = config.get(SOCKET_BUFFER_SIZE),
        .bytesUsedForSocketBufferSizeTransfer = config.get(SOCKET_BUFFER_TRANSFER_SIZE),
        .flushIntervalInMs = config.get(FLUSH_INTERVAL_MS),
        .connectTimeoutSeconds = config.get(CONNECT_TIMEOUT_SECONDS)};
}

TCPSource::TCPSource(const TCPSourceConfig& config)
    : errBuffer{}
    , socketHost(config.socketHost)
    , socketPort(std::to_string(config.socketPort))
    , socketType(config.socketType)
    , socketDomain(config.socketDomain)
    , tupleDelimiter(config.tupleDelimiter)
    , socketBufferSize(config.socketBufferSize)
    , bytesUsedForSocketBufferSizeTransfer(config.bytesUsedForSocketBufferSizeTransfer)
    , flushIntervalInMs(config.flushIntervalInMs)
    , connectionTimeout(config.connectTimeoutSeconds)
{
    NES_TRACE("Init TCPSource.");
}

std::ostream& TCPSource::toString(std::ostream& str) const
{
    str << "\nTCPSource(";
    str << "\n  generated tuples: " << this->generatedTuples;
    str << "\n  generated buffers: " << this->generatedBuffers;
    str << "\n  connection: " << this->connection;
    str << "\n  timeout: " << connectionTimeout << " seconds";
    str << "\n  socketHost: " << socketHost;
    str << "\n  socketPort: " << socketPort;
    str << "\n  socketType: " << socketType;
    str << "\n  socketDomain: " << socketDomain;
    str << "\n  tupleDelimiter: " << tupleDelimiter;
    str << "\n  socketBufferSize: " << socketBufferSize;
    str << "\n  bytesUsedForSocketBufferSizeTransfer" << bytesUsedForSocketBufferSizeTransfer;
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

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterTCPInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    std::unordered_map<Identifier, ConfigLiteral> defaultSourceConfig{{Identifier::parse("flush_interval_ms"), int64_t{100}}};
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.merge(defaultSourceConfig);

    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(SOCKET_PORT.getName()))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(SOCKET_HOST.getName()))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.tuples));

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        SOCKET_PORT.getName(), static_cast<int64_t>(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(SOCKET_HOST.getName(), "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterTCPFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    std::unordered_map<Identifier, ConfigLiteral> defaultSourceConfig{{Identifier::parse("flush_interval_ms"), int64_t{100}}};
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.merge(defaultSourceConfig);

    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(SOCKET_PORT.getName()))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(SOCKET_HOST.getName()))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(systestAdaptorArguments.testFilePath);

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        SOCKET_PORT.getName(), static_cast<int64_t>(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(SOCKET_HOST.getName(), "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}
}
