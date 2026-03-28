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

#include <cerrno>
#include <chrono>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <fcntl.h>
#include <netdb.h>
#include <sys/select.h>
#include <unistd.h>

namespace NES
{

TCPSink::TCPSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , socketHost(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::HOST))
    , socketPort(std::to_string(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::PORT)))
    , socketType(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::TYPE))
    , socketDomain(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::DOMAIN))
    , connectionTimeout(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::CONNECT_TIMEOUT))
{
}

std::ostream& TCPSink::toString(std::ostream& str) const
{
    str << fmt::format("TCPSink({}:{}, domain: {}, type: {})", socketHost, socketPort, socketDomain, socketType);
    return str;
}

void TCPSink::tryToConnect(const addrinfo* result)
{
    const std::chrono::seconds socketConnectDefaultTimeout{connectionTimeout};
    while (result != nullptr)
    {
        socketFileDescriptor = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
        if (socketFileDescriptor != -1)
        {
            break;
        }
        result = result->ai_next;
    }

    if (result == nullptr)
    {
        throw CannotOpenSink("No valid address found to create TCP sink socket for {}:{}", socketHost, socketPort);
    }

    const int flags = fcntl(socketFileDescriptor, F_GETFL, 0);
    fcntl(socketFileDescriptor, F_SETFL, flags | O_NONBLOCK); /// NOLINT(cppcoreguidelines-pro-type-vararg)

    timeval timeout{.tv_sec = static_cast<time_t>(socketConnectDefaultTimeout.count()), .tv_usec = IMPLICIT_TIMEOUT_USEC};
    setsockopt(socketFileDescriptor, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    auto connection = connect(socketFileDescriptor, result->ai_addr, result->ai_addrlen);
    if (connection < 0)
    {
        if (errno != EINPROGRESS)
        {
            closeSocket();
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSink("Could not connect to: {}:{}. {}", socketHost, socketPort, strerrorResult);
        }

        fd_set fdset;
        timeval timeValue{.tv_sec = static_cast<time_t>(socketConnectDefaultTimeout.count()), .tv_usec = IMPLICIT_TIMEOUT_USEC};

        FD_ZERO(&fdset);
        FD_SET(socketFileDescriptor, &fdset);

        connection = select(socketFileDescriptor + 1, nullptr, &fdset, nullptr, &timeValue);
        if (connection <= 0)
        {
            errno = ETIMEDOUT;
            closeSocket();
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSink("Could not connect to: {}:{}. {}", socketHost, socketPort, strerrorResult);
        }

        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(socketFileDescriptor, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0)
        {
            errno = error;
            closeSocket();
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSink("Could not connect to: {}:{}. {}", socketHost, socketPort, strerrorResult);
        }
    }

    fcntl(socketFileDescriptor, F_SETFL, flags); /// NOLINT(cppcoreguidelines-pro-type-vararg)
}

void TCPSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up tcp sink: {}", *this);

    addrinfo hints{};
    addrinfo* result = nullptr;
    hints.ai_family = socketDomain;
    hints.ai_socktype = socketType;
    hints.ai_flags = 0;
    hints.ai_protocol = 0;

    const auto errorCode = getaddrinfo(socketHost.c_str(), socketPort.c_str(), &hints, &result);
    if (errorCode != 0)
    {
        throw CannotOpenSink("Failed getaddrinfo with error: {}", gai_strerror(errorCode));
    }

    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> resultGuard(result, freeaddrinfo);
    tryToConnect(result);
    isOpen = true;
}

void TCPSink::sendAll(const char* data, size_t length)
{
    size_t sentBytes = 0;
    while (sentBytes < length)
    {
        const auto writtenBytes = send(socketFileDescriptor, data + sentBytes, length - sentBytes, MSG_NOSIGNAL);
        if (writtenBytes < 0)
        {
            const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSink("Failed to write to TCP sink {}:{}: {}", socketHost, socketPort, strerrorResult);
        }
        if (writtenBytes == 0)
        {
            throw CannotOpenSink("TCP sink connection {}:{} was closed while sending", socketHost, socketPort);
        }
        sentBytes += static_cast<size_t>(writtenBytes);
    }
}

void TCPSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in TCPSink.");
    PRECONDITION(isOpen, "Sink was not opened");

    BufferIterator iterator{inputTupleBuffer};
    std::optional<BufferIterator::BufferElement> element = iterator.getNextElement();
    while (element.has_value())
    {
        sendAll(element.value().buffer.getAvailableMemoryArea<char>().data(), element.value().contentLength);
        element = iterator.getNextElement();
    }
}

void TCPSink::closeSocket()
{
    if (socketFileDescriptor >= 0)
    {
        ::close(socketFileDescriptor);
        socketFileDescriptor = -1;
    }
    isOpen = false;
}

void TCPSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Closing tcp sink {}:{}", socketHost, socketPort);
    closeSocket();
}

DescriptorConfig::Config TCPSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTCPSink>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterTCPSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return TCPSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterTCPSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<TCPSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
