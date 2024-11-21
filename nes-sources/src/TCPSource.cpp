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

#include <cerrno> /// For socket error
#include <chrono>
#include <cstring>
#include <exception>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <netdb.h>
#include <unistd.h> /// For read
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <asm-generic/socket.h>
#include <bits/types/struct_timeval.h>
#include <sys/socket.h> /// For socket functions
#include <sys/types.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <TCPSource.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>


namespace NES::Sources
{

TCPSource::TCPSource(const SourceDescriptor& sourceDescriptor)
    : socketHost(sourceDescriptor.getFromConfig(ConfigParametersTCP::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersTCP::PORT)))
    , socketType(sourceDescriptor.getFromConfig(ConfigParametersTCP::TYPE))
    , socketDomain(sourceDescriptor.getFromConfig(ConfigParametersTCP::DOMAIN))
    , tupleDelimiter(sourceDescriptor.getFromConfig(ConfigParametersTCP::SEPARATOR))
    , socketBufferSize(sourceDescriptor.getFromConfig(ConfigParametersTCP::SOCKET_BUFFER_SIZE))
    , bytesUsedForSocketBufferSizeTransfer(sourceDescriptor.getFromConfig(ConfigParametersTCP::SOCKET_BUFFER_TRANSFER_SIZE))
    , flushIntervalInMs(sourceDescriptor.getFromConfig(ConfigParametersTCP::FLUSH_INTERVAL_MS))
{
    /// init physical types
    std::vector<std::string> const schemaKeys;
    const DefaultPhysicalTypeFactory defaultPhysicalTypeFactory{};

    NES_TRACE("TCPSource::TCPSource: Init TCPSource.");
}

std::ostream& TCPSource::toString(std::ostream& str) const
{
    str << "\nTCPSource(";
    str << "\n  generated tuples: " << this->generatedTuples;
    str << "\n  generated buffers: " << this->generatedBuffers;
    str << "\n  connection: " << this->connection;
    str << "\n  timeout: " << TCP_SOCKET_DEFAULT_TIMEOUT.count() << " microseconds";
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

void TCPSource::open()
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

    /// Try each address until we successfully connect
    while (result != nullptr)
    {
        sockfd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
        if (sockfd == -1)
        {
            result = result->ai_next;
            continue;
        }

        constexpr static timeval Timeout{0, TCP_SOCKET_DEFAULT_TIMEOUT.count()};
        setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &Timeout, sizeof(Timeout));
        connection = connect(sockfd, result->ai_addr, result->ai_addrlen);

        if (connection != -1)
        {
            break; /// success
        }

        /// The connection was closed, therefore we close the source.
        close();
    }
    freeaddrinfo(result);

    if (result == nullptr)
    {
        throw CannotOpenSource("Could not connect to: {}:", socketHost, socketPort);
    }

    NES_TRACE("TCPSource::open: Connected to server.");
}

size_t TCPSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer)
{
    try
    {
        size_t numReceivedBytes = 0;
        while (fillBuffer(tupleBuffer, numReceivedBytes))
        {
            /// Fill the buffer until EoS reached or the number of tuples in the buffer is not equals to 0.
        };
        return numReceivedBytes;
    }
    catch (const std::exception& e)
    {
        NES_ERROR("TCPSource::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw e;
    }
}

bool TCPSource::fillBuffer(NES::Memory::TupleBuffer& tupleBuffer, size_t& numReceivedBytes)
{
    const auto flushIntervalTimerStart = std::chrono::system_clock::now();
    bool flushIntervalPassed = false;
    bool readWasValid = true;

    const size_t rawTBSize = tupleBuffer.getBufferSize();
    while (not flushIntervalPassed and numReceivedBytes < rawTBSize)
    {
        const ssize_t bufferSizeReceived = read(sockfd, tupleBuffer.getBuffer() + numReceivedBytes, rawTBSize - numReceivedBytes);
        numReceivedBytes += bufferSizeReceived;
        if (bufferSizeReceived == INVALID_RECEIVED_BUFFER_SIZE)
        {
            /// if read method returned -1 an error occurred during read.
            NES_ERROR("An error occurred while reading from socket. Error: {}", strerror(errno));
            readWasValid = false;
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
            NES_DEBUG("TCPSource::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
    }
    ++generatedBuffers;
    /// Loop while we haven't received any bytes yet and we can still read from the socket.
    return numReceivedBytes == 0 and readWasValid;
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
TCPSource::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), NAME);
}

void TCPSource::close()
{
    NES_TRACE("TCPSource::close: trying to close connection.");
    if (connection >= 0)
    {
        ::close(connection);
        ::close(sockfd);
        NES_TRACE("TCPSource::close: connection closed.");
    }
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SourceValidationGeneratedRegistrar::RegisterSourceValidationTCP(std::unordered_map<std::string, std::string>&& sourceConfig)
{
    return TCPSource::validateAndFormat(std::move(sourceConfig));
}

std::unique_ptr<Source> SourceGeneratedRegistrar::RegisterTCPSource(const SourceDescriptor& sourceDescriptor)
{
    return std::make_unique<TCPSource>(sourceDescriptor);
}

}
