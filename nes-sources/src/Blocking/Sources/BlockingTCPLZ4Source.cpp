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

#include <Blocking/Sources/BlockingTCPLZ4Source.hpp>

#include <cerrno> /// For socket error
#include <chrono>
#include <cstring>
#include <exception>
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
#include <unistd.h> /// For read
#include <Configurations/Descriptor.hpp>
#include <DataServer/TCPDataServer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <Util/Logger/Logger.hpp>
#include <asm-generic/socket.h>
#include <bits/types/struct_timeval.h>
#include <cpptrace/from_current.hpp>
#include <sys/socket.h> /// For socket functions
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <lz4.h>
#include <lz4frame.h>

namespace NES::Sources
{

BlockingTCPLZ4Source::BlockingTCPLZ4Source(const SourceDescriptor& sourceDescriptor)
    : socketHost(sourceDescriptor.getFromConfig(ConfigParametersBlockingTCPLZ4::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersBlockingTCPLZ4::PORT)))
    , socketType(sourceDescriptor.getFromConfig(ConfigParametersBlockingTCPLZ4::TYPE))
    , socketDomain(sourceDescriptor.getFromConfig(ConfigParametersBlockingTCPLZ4::DOMAIN))
    , tupleDelimiter(sourceDescriptor.getFromConfig(ConfigParametersBlockingTCPLZ4::SEPARATOR))
    , socketBufferSize(sourceDescriptor.getFromConfig(ConfigParametersBlockingTCPLZ4::SOCKET_BUFFER_SIZE))
    , bytesUsedForSocketBufferSizeTransfer(sourceDescriptor.getFromConfig(ConfigParametersBlockingTCPLZ4::SOCKET_BUFFER_TRANSFER_SIZE))
    , flushIntervalInMs(sourceDescriptor.getFromConfig(ConfigParametersBlockingTCPLZ4::FLUSH_INTERVAL_MS))
{
    /// init physical types
    const std::vector<std::string> schemaKeys;
    NES_TRACE("TCPLZ4Source::TCPLZ4Source: Init TCPLZ4Source.");

    /// properly create the decompression context
    auto errorCode = LZ4F_createDecompressionContext(&decompCtx, LZ4F_VERSION);
    if (LZ4F_isError(errorCode))
    {
        NES_ERROR("Failed to create decompression context for TCPLZ4 source");
    }
    /// Allocate space for 64kb compressed data at once
    srcBuffer.reserve(64 * 1024);
}

std::ostream& BlockingTCPLZ4Source::toString(std::ostream& str) const
{
    str << "\nTCPLZ4Source(";
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

void BlockingTCPLZ4Source::open()
{
    NES_TRACE("Trying to create socket and connect.");

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

    LZ4F_resetDecompressionContext(decompCtx);
    srcBufferPos = 0;
    receivedCompressedBytes = 0;
    NES_DEBUG("BlockingTCPLZ4Source::open: Connected to server.");
}

size_t BlockingTCPLZ4Source::fillBuffer(IOBuffer& tupleBuffer, const std::stop_token&)
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
        NES_ERROR("TCPLZ4Source::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw e;
    }
}

bool BlockingTCPLZ4Source::fillBuffer(Memory::TupleBuffer& tupleBuffer, size_t& numReceivedBytes)
{
    const auto flushIntervalTimerStart = std::chrono::system_clock::now();
    bool flushIntervalPassed = false;
    bool readWasValid = true;

    const size_t rawTBSize = tupleBuffer.getBufferSize();
    while (not flushIntervalPassed and numReceivedBytes < rawTBSize)
    {
        /// If there is no data left in the last compressed data buffer, read new compressed data fromn source
        if (srcBufferPos == receivedCompressedBytes)
        {
            /// Fill up the buffer for compressed data again and reset the srcBuffer pos
            receivedCompressedBytes = read(sockfd, srcBuffer.data(), srcBuffer.capacity());
            if (receivedCompressedBytes == INVALID_RECEIVED_BUFFER_SIZE)
            {
                /// if read method returned -1 an error occurred during read.
                NES_ERROR("An error occurred while reading from socket. Error: {}", strerror(errno));
                readWasValid = false;
                numReceivedBytes = 0;
                break;
            }

            if (receivedCompressedBytes == EOF_RECEIVED_BUFFER_SIZE)
            {
                NES_INFO("No data received from {}:{}.", socketHost, socketPort);
                if (numReceivedBytes == 0)
                {
                    NES_INFO("TCPLZ4Source detected EoS");
                    readWasValid = false;
                }
                break;
            }
            srcBufferPos = 0;
        }
        /// We decode the contents of the compressed source buffer into the tuple buffer until either the tuple buffer is full or the source buffer is empty
        /// Calculate current remaining space in the tuple buffer and the remaining bytes to be decompressed in the src buffer
        size_t remaingSrcBytes = receivedCompressedBytes - srcBufferPos;
        size_t remainingDstCapacity = rawTBSize - numReceivedBytes;

        /// Decompress into tuple buffer
        auto stateCode = LZ4F_decompress(
            decompCtx,
            tupleBuffer.getBuffer() + numReceivedBytes,
            &remainingDstCapacity,
            srcBuffer.data() + srcBufferPos,
            &remaingSrcBytes,
            nullptr);

        if (LZ4F_isError(stateCode)) {
            NES_ERROR("Something went wrong during decompression.")
            readWasValid = false;
            break;
        }

        /// remainingSrcBytes now contains the amount of compressed bytes that were decompressed. We move the pos accordingly
        srcBufferPos += remaingSrcBytes;

        /// remainingDstCapacity now contains the amount of decompressed bytes that we wrote into the tuple buffer. Increase numReceivedBytes accordingly
        numReceivedBytes += remainingDstCapacity;

        /// If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
        /// and writing data exceeds the user defined limit (bufferFlushIntervalMs).
        /// If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
        /*
        if ((flushIntervalInMs > 0
             && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - flushIntervalTimerStart).count()
                 >= flushIntervalInMs))
        {
            NES_DEBUG("TCPLZ4Source::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
        */
    }
    ++generatedBuffers;
    /// Loop while we haven't received any bytes yet and we can still read from the socket.
    return numReceivedBytes == 0 and readWasValid;
}

DescriptorConfig::Config BlockingTCPLZ4Source::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersBlockingTCPLZ4>(std::move(config), NAME);
}

void BlockingTCPLZ4Source::close()
{
    NES_DEBUG("TCPLZ4Source::close: trying to close connection.");
    if (connection >= 0)
    {
        LZ4F_freeDecompressionContext(decompCtx);
        ::close(sockfd);
        NES_TRACE("TCPLZ$Source::close: connection closed.");
    }
}

SourceValidationRegistryReturnType SourceValidationGeneratedRegistrar::RegisterBlockingTCPLZ4SourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return BlockingTCPLZ4Source::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterBlockingTCPLZ4Source(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<BlockingTCPLZ4Source>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterBlockingTCPLZ4InlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.attachSource.tuples)
    {
        if (const auto port = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(ConfigParametersBlockingTCPLZ4::PORT);
            port != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.attachSource.tuples.value()));
            port->second = std::to_string(mockTCPServer->getPort());

            if (const auto host = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(ConfigParametersBlockingTCPLZ4::HOST);
                host != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
            {
                host->second = "localhost";
                auto serverThread
                    = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
                systestAdaptorArguments.attachSource.serverThreads->push_back(std::move(serverThread));

                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw InvalidConfigParameter("A TCP source config must contain a 'host' parameter");
        }
        throw InvalidConfigParameter("A TCP source config must contain a 'port' parameter");
    }
    throw TestException("An INLINE SystestAttachSource must not have a 'tuples' vector that is null.");
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterBlockingTCPLZ4FileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (const auto attachSourceFilePath = systestAdaptorArguments.attachSource.fileDataPath)
    {
        if (const auto port = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(ConfigParametersBlockingTCPLZ4::PORT);
            port != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            auto mockTCPServer = std::make_unique<TCPDataServer>(attachSourceFilePath.value());
            port->second = std::to_string(mockTCPServer->getPort());

            if (const auto host = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(ConfigParametersBlockingTCPLZ4::HOST);
                host != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
            {
                host->second = "localhost";
                auto serverThread
                    = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
                systestAdaptorArguments.attachSource.serverThreads->push_back(std::move(serverThread));

                systestAdaptorArguments.physicalSourceConfig.sourceConfig.erase(std::string(SYSTEST_FILE_PATH_PARAMETER));
                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw InvalidConfigParameter("A TCPLZ4Source config must contain a 'host' parameter");
        }
        throw InvalidConfigParameter("A TCPLZ4Source config must contain a 'port' parameter");
    }
    throw InvalidConfigParameter("An attach source of type FileData must contain a filePath configuration.");
}
}
