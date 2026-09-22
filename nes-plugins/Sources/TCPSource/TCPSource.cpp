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
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
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

TCPSource::TCPSource(const SourceDescriptor& sourceDescriptor)
    : errBuffer{}
    , socketHost(sourceDescriptor.getFromConfig(ConfigParametersTCP::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersTCP::PORT)))
    , socketType(sourceDescriptor.getFromConfig(ConfigParametersTCP::TYPE))
    , socketDomain(sourceDescriptor.getFromConfig(ConfigParametersTCP::DOMAIN))
    , tupleDelimiter(sourceDescriptor.getFromConfig(ConfigParametersTCP::SEPARATOR))
    , socketBufferSize(sourceDescriptor.getFromConfig(ConfigParametersTCP::SOCKET_BUFFER_SIZE))
    , bytesUsedForSocketBufferSizeTransfer(sourceDescriptor.getFromConfig(ConfigParametersTCP::SOCKET_BUFFER_TRANSFER_SIZE))
    , flushIntervalInMs(sourceDescriptor.getFromConfig(ConfigParametersTCP::FLUSH_INTERVAL_MS))
    , connectionTimeout(sourceDescriptor.getFromConfig(ConfigParametersTCP::CONNECT_TIMEOUT))
{
    NES_TRACE("Init TCPSource.");
}

TCPSource::TCPSource(InjectedSocketTag, const int connectedSocketFd, std::string host, std::string port, const float flushIntervalMs)
    : connection(connectedSocketFd)
    , sockfd(connectedSocketFd)
    , errBuffer{}
    , socketHost(std::move(host))
    , socketPort(std::move(port))
    , socketType(SOCK_STREAM)
    , socketDomain(AF_INET)
    , tupleDelimiter('\n')
    , socketBufferSize(1024)
    , bytesUsedForSocketBufferSizeTransfer(0)
    , flushIntervalInMs(flushIntervalMs)
    , connectionTimeout(0)
{
    NES_TRACE("Init TCPSource around injected socket fd (test-only).");
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

bool TCPSource::tryToConnect(const addrinfo* result)
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

    /// Capture the socket's original flags now that sockfd actually exists (a pre-connect() fcntl(F_GETFL)
    /// on a not-yet-created fd reads garbage, corrupting the blocking-mode restore below). A -1 result means
    /// F_GETFL itself failed; fall back to 0 so the restore below doesn't propagate that failure as bits.
    originalFlags = fcntl(sockfd, F_GETFL, 0);
    if (originalFlags == -1)
    {
        originalFlags = 0;
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg, hicpp-signed-bitwise) - POSIX API requires varargs
    fcntl(sockfd, F_SETFL, originalFlags | O_NONBLOCK);

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

    CPPTRACE_TRY
    {
        tryToConnect(result);
    }
    CPPTRACE_CATCH(...)
    {
        ::close(sockfd); /// close socket to clean up state
        throw wrapExternalException("Could not establich connection!");
    }

    /// Restore blocking mode (cleared during connect() above) so SO_RCVTIMEO governs read() timeouts again.
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg) - POSIX API requires varargs
    fcntl(sockfd, F_SETFL, originalFlags & ~O_NONBLOCK);

    NES_TRACE("TCPSource::open: Connected to server.");
}

TCPSource::ReadOutcome TCPSource::classifyReadResult(const ssize_t bytesReceived, const int errnoValue)
{
    if (bytesReceived > 0)
    {
        return ReadOutcome::Data;
    }
    if (bytesReceived == EOF_RECEIVED_BUFFER_SIZE)
    {
        return ReadOutcome::EndOfStream;
    }
    /// bytesReceived < 0: a genuine read() failure. Distinguish transient conditions from a real error so a
    /// mid-stream failure (e.g. ECONNRESET) is never silently reported as a clean end-of-stream.
    if (errnoValue == EINTR)
    {
        return ReadOutcome::Retry;
    }
    if (errnoValue == EAGAIN || errnoValue == EWOULDBLOCK)
    {
        return ReadOutcome::WouldBlock;
    }
    return ReadOutcome::Error;
}

Source::FillTupleBufferResult TCPSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    try
    {
        size_t numReceivedBytes = 0;
        while (fillBuffer(tupleBuffer, numReceivedBytes, stopToken))
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

bool TCPSource::fillBuffer(TupleBuffer& tupleBuffer, size_t& numReceivedBytes, const std::stop_token& stopToken)
{
    const auto flushIntervalTimerStart = std::chrono::system_clock::now();
    bool flushIntervalPassed = false;
    bool readWasValid = true;

    const size_t rawTBSize = tupleBuffer.getBufferSize();
    while (not flushIntervalPassed and numReceivedBytes < rawTBSize and not stopToken.stop_requested())
    {
        const ssize_t bufferSizeReceived
            = read(sockfd, tupleBuffer.getAvailableMemoryArea().data() + numReceivedBytes, rawTBSize - numReceivedBytes);
        switch (classifyReadResult(bufferSizeReceived, errno))
        {
            case ReadOutcome::Data: {
                numReceivedBytes += bufferSizeReceived;
                break;
            }
            case ReadOutcome::EndOfStream: {
                NES_TRACE("No data received from {}:{}.", socketHost, socketPort);
                if (numReceivedBytes == 0)
                {
                    NES_INFO("TCP Source detected EoS");
                    readWasValid = false;
                }
                flushIntervalPassed = true;
                continue;
            }
            case ReadOutcome::Retry: {
                continue;
            }
            case ReadOutcome::WouldBlock: {
                if (numReceivedBytes > 0)
                {
                    /// Hand back what we already have rather than blocking further; the caller will call in again.
                    return true;
                }
                /// Nothing to read yet: back off briefly instead of busy-polling an idle/slow peer at 100% CPU.
                std::this_thread::sleep_for(IDLE_POLL_BACKOFF);
                continue;
            }
            case ReadOutcome::Error: {
                /// A genuine socket error (e.g. ECONNRESET) must not be reported as a clean end-of-stream:
                /// that would silently complete the query with truncated data instead of failing it.
                const auto strerrorResult = strerror_r(errno, errBuffer.data(), errBuffer.size());
                throw RunningRoutineFailure("Error while reading from socket {}:{}. {}", socketHost, socketPort, strerrorResult);
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
    return numReceivedBytes == 0 and readWasValid and not stopToken.stop_requested();
}

DescriptorConfig::Config TCPSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), name());
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

    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse(ConfigParametersTCP::PORT)))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse(ConfigParametersTCP::HOST)))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.tuples));

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        Identifier::parse(ConfigParametersTCP::PORT), std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(Identifier::parse(ConfigParametersTCP::HOST), "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

FileDataRegistryReturnType TCPSource::provideFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    std::unordered_map<Identifier, std::string> defaultSourceConfig{{Identifier::parse("flush_interval_ms"), "100"}};
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.merge(defaultSourceConfig);

    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse(ConfigParametersTCP::PORT)))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(Identifier::parse(ConfigParametersTCP::HOST)))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }


    auto mockTCPServer = std::make_unique<TCPDataServer>(systestAdaptorArguments.testFilePath);

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        Identifier::parse(ConfigParametersTCP::PORT), std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(Identifier::parse(ConfigParametersTCP::HOST), "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}
}
