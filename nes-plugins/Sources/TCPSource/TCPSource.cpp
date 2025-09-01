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

#include <array>
#include <cerrno> /// For socket error
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <ratio>
#include <span>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <fcntl.h>
#include <netdb.h>
#include <unistd.h> /// For read
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <asm-generic/socket.h>
#include <bits/types/struct_timeval.h>
#include <fmt/format.h>
#include <sys/select.h>
#include <sys/socket.h> /// For socket functions
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <TCPDataServer.hpp>

namespace
{
::timeval toTimeval(std::chrono::microseconds duration)
{
    ::timeval dest{};
    auto millisecs = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    dest.tv_sec = millisecs.count() / 1000;
    dest.tv_usec = (millisecs.count() % 1000) * 1000;
    return dest;
}

std::string toErrorString(int error)
{
    std::array<char, 1024> buffer{};
    const char* result = ::strerror_r(error, buffer.data(), buffer.size());
    return result;
}
}

namespace NES
{
auto Socket::setNonBlock()
{
    return addFlag(O_NONBLOCK);
}

auto Socket::setBlock()
{
    return removeFlag(O_NONBLOCK);
}

std::expected<void, std::string> Socket::setTimeouts(std::chrono::microseconds timeoutDuration)
{
    if (timeoutDuration == timeout)
    {
        return {};
    }

    auto timeout = toTimeval(timeoutDuration);
    if (setsockopt(static_cast<int>(descriptor), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == -1)
    {
        return std::unexpected(fmt::format("could not set socket recv timeout: {}", toErrorString(errno)));
    }

    if (setsockopt(static_cast<int>(descriptor), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0)
    {
        return std::unexpected(fmt::format("could not set socket send timeout: {}", toErrorString(errno)));
    }
    this->timeout = std::chrono::duration_cast<std::chrono::microseconds>(timeoutDuration);
    return {};
}

std::expected<Connection, std::string> Connection::from(Socket sock, AddressInfo addrinfo, std::chrono::microseconds connectionTimeout)
{
    /// In order to provide a timeout for the `connect` operation, we have to use the nonblocking connect version which we can race
    /// against a timeout using `select`.

    if (auto nonBlock = sock.setNonBlock(); !nonBlock)
    {
        return std::unexpected(fmt::format("Failed to set socket to non-blocking: {}", nonBlock.error()));
    }

    if (connect(static_cast<int>(sock.descriptor), addrinfo->ai_addr, addrinfo->ai_addrlen) != -1)
    {
        sock.setBlock();
        return Connection{.socket = std::move(sock), .addrinfo = std::move(addrinfo), .connectionTimeout = connectionTimeout};
    }

    if (errno != EINPROGRESS)
    {
        return std::unexpected(fmt::format("Failed to connect to with error: {}", toErrorString(errno)));
    }

    /// Set the timeout for the connect attempt
    auto timeout = toTimeval(connectionTimeout);
    fd_set fdset;
    FD_ZERO(&fdset);
    FD_SET(static_cast<int>(sock.descriptor), &fdset);
    auto selectResult = select(static_cast<int>(sock.descriptor) + 1, nullptr, &fdset, nullptr, &timeout);
    if (selectResult == -1)
    {
        return std::unexpected(fmt::format("Failed to select with error: {}", toErrorString(errno)));
    }
    if (selectResult == 0)
    {
        return std::unexpected("Timeout while connecting to socket");
    }
    if (selectResult != 1)
    {
        return std::unexpected(fmt::format("Unexpected result from select: {}. Expected exactly one file descriptor", selectResult));
    }
    auto sockError = sock.checkError();
    if (!sockError)
    {
        return std::unexpected(fmt::format("When connecting: {}", sockError.error()));
    }

    auto setBlockError = sock.setBlock();
    if (!setBlockError)
    {
        return std::unexpected(fmt::format("Failed to set socket to blocking: {}", setBlockError.error()));
    }

    return Connection{
        .socket = std::move(sock),
        .addrinfo = std::move(addrinfo),
        .connectionTimeout = std::chrono::duration_cast<std::chrono::microseconds>(connectionTimeout)};
}

std::expected<AddressInfo, std::string> AddressInfo::from(const std::string& host, uint16_t port)
{
    addrinfo hints{};
    addrinfo* result = nullptr;

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0; /// use default behavior
    hints.ai_protocol
        = 0; /// specifying 0 in this field indicates that socket addresses with any protocol can be returned by getaddrinfo() ;

    const std::string portNumber = std::to_string(port);
    const auto errorCode = getaddrinfo(host.c_str(), portNumber.c_str(), &hints, &result);
    if (errorCode != 0)
    {
        return std::unexpected(fmt::format("Failed getaddrinfo with error: {}", gai_strerror(errorCode)));
    }

    return AddressInfo{.result = std::unique_ptr<addrinfo, decltype(&freeaddrinfo)>{result, &freeaddrinfo}};
}

OwnedDescriptor::OwnedDescriptor(int descriptor) : descriptor(descriptor)
{
}

OwnedDescriptor::~OwnedDescriptor()
{
    if (descriptor != -1)
    {
        ::close(descriptor);
    }
}

OwnedDescriptor::OwnedDescriptor(OwnedDescriptor&& other) noexcept : descriptor(other.descriptor)
{
    other.descriptor = -1;
}

OwnedDescriptor& OwnedDescriptor::operator=(OwnedDescriptor&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    descriptor = other.descriptor;
    other.descriptor = -1;
    return *this;
}

std::expected<Socket, std::string> Socket::create(const AddressInfo& address)
{
    auto sockfd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (sockfd == -1)
    {
        return std::unexpected(fmt::format("Failed to create socket with error: {}", toErrorString(errno)));
    }
    const int flags = fcntl(sockfd, F_GETFL, 0);
    return Socket{
        .descriptor = OwnedDescriptor(sockfd), .flags = static_cast<unsigned>(flags), .timeout = std::chrono::microseconds::zero()};
}

std::expected<void, std::string> Socket::addFlag(unsigned flag)
{
    if ((flags & flag) == 0)
    {
        if (fcntl(static_cast<int>(descriptor), F_SETFL, flags | flag) == 0)
        {
            flags |= flag;
        }
        else
        {
            return std::unexpected(fmt::format("could not set socket flag `{}`: {}", flag, toErrorString(errno)));
        }
    }
    return {};
}

std::expected<void, std::string> Socket::removeFlag(unsigned flag)
{
    if ((flags & flag) == 1)
    {
        if (fcntl(static_cast<int>(descriptor), F_SETFL, flags & ~flag) == 0)
        {
            flags &= ~flag;
        }
        else
        {
            return std::unexpected(fmt::format("could not remove socket flag `{}`: {}", flag, toErrorString(errno)));
        }
    }
    return {};
}

std::expected<void, std::string> Socket::checkError() const
{
    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(static_cast<int>(descriptor), SOL_SOCKET, SO_ERROR, &error, &len) == -1)
    {
        return std::unexpected(fmt::format("Failed to get socket error with error: {}", toErrorString(errno)));
    }

    if (error != 0)
    {
        return std::unexpected(fmt::format("Socket has error: {}", toErrorString(error)));
    }

    return {};
}

std::expected<void, std::string> Connection::reconnect()
{
    auto result = Connection::from(std::move(this->socket), std::move(this->addrinfo), connectionTimeout);
    if (result)
    {
        this->socket = std::move(result->socket);
        this->addrinfo = std::move(result->addrinfo);
        this->connectionTimeout = result->connectionTimeout;
        return {};
    }

    return std::unexpected(fmt::format("Failed to reconnect to socket: {}", result.error()));
}

std::expected<std::span<uint8_t>, std::variant<Connection::EoS, std::string>>
Connection::receive(std::span<uint8_t> result, std::chrono::microseconds timeout)
{
    if (auto result = socket.setTimeouts(timeout); !result.has_value())
    {
        return std::unexpected(std::move(result.error()));
    }

    while (true)
    {
        auto resultSize = recv(static_cast<int>(socket.descriptor), result.data(), result.size(), 0);
        if (resultSize == -1)
        {
            if (errno == EAGAIN || errno == EINTR)
            {
                continue;
            }
            auto errorMessage = toErrorString(errno);
            auto reconnectResult = reconnect();
            if (reconnectResult)
            {
                continue;
            }
            return std::unexpected(
                fmt::format("Failed to receive from socket with error: {}. Reconnect failed: {}", errorMessage, reconnectResult.error()));
        }
        if (resultSize == 0)
        {
            return std::unexpected(EoS{});
        }
        return std::span(result.data(), resultSize);
    }
}

TCPSource::TCPSource(const SourceDescriptor& sourceDescriptor)
    : socketHost(sourceDescriptor.getFromConfig(ConfigParametersTCP::HOST))
    , socketPort(sourceDescriptor.getFromConfig(ConfigParametersTCP::PORT))
    , socketType(sourceDescriptor.getFromConfig(ConfigParametersTCP::TYPE))
    , socketDomain(sourceDescriptor.getFromConfig(ConfigParametersTCP::DOMAIN))
    , flushIntervalInMs(sourceDescriptor.getFromConfig(ConfigParametersTCP::FLUSH_INTERVAL_MS))
    , connectionTimeout(sourceDescriptor.getFromConfig(ConfigParametersTCP::CONNECT_TIMEOUT))
{
    NES_TRACE("Init TCPSource.");
}

std::ostream& TCPSource::toString(std::ostream& str) const
{
    str << "\nTCPSource(";
    str << "\n  generated buffers: " << this->generatedBuffers;
    str << "\n  timeout: " << connectionTimeout << " seconds";
    str << "\n  socketHost: " << socketHost;
    str << "\n  socketPort: " << socketPort;
    str << "\n  socketType: " << socketType;
    str << "\n  socketDomain: " << socketDomain;
    str << "\n  flushIntervalInMs" << flushIntervalInMs;
    str << ")\n";
    return str;
}

void TCPSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_TRACE("TCPSource::open: Trying to create socket and connect.");

    auto addressInfo = AddressInfo::from(socketHost, socketPort);
    if (!addressInfo)
    {
        throw CannotOpenSource("Could not resolve host: {}", addressInfo.error());
    }

    auto socket = Socket::create(*addressInfo);
    if (!socket)
    {
        throw CannotOpenSource("Could not create socket: {}", socket.error());
    }

    auto connection = Connection::from(std::move(*socket), std::move(*addressInfo), std::chrono::seconds(connectionTimeout));

    if (!connection)
    {
        throw CannotOpenSource("Could not connect to server: {}", connection.error());
    }

    this->connection = std::move(connection.value());

    NES_TRACE("TCPSource::open: Connected to server.");
}

Source::FillTupleBufferResult TCPSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    auto receiveResult = connection.value().receive(
        tupleBuffer.getAvailableMemoryArea<uint8_t>(),
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::duration<float, std::milli>(flushIntervalInMs)));
    if (receiveResult)
    {
        ++generatedBuffers;
        return FillTupleBufferResult(receiveResult->size());
    }

    if (std::holds_alternative<Connection::EoS>(receiveResult.error()))
    {
        return {};
    }

    throw CannotOpenSource("Failed to receive from socket: {}", std::get<std::string>(receiveResult.error()));
}

DescriptorConfig::Config TCPSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), name());
}

void TCPSource::close()
{
    connection = {};
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
