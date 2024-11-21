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

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <errno.h> /// For socket error
#include <netdb.h>
#include <unistd.h> /// For read
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SourceParsers/SourceParser.hpp>
#include <SourceParsers/SourceParserCSV.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceRegistry.hpp>
#include <Sources/SourceTCP.hpp>
#include <SourcesValidation/SourceRegistryValidation.hpp>
#include <sys/socket.h> /// For socket functions
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

#include <magic_enum.hpp>

namespace NES::Sources
{

SourceTCP::SourceTCP(const SourceDescriptor& sourceDescriptor)
    : circularBuffer(getpagesize() * 2)
    , inputFormat(sourceDescriptor.getFromConfig(ConfigParametersTCP::INPUT_FORMAT))
    , socketHost(sourceDescriptor.getFromConfig(ConfigParametersTCP::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersTCP::PORT)))
    , socketType(sourceDescriptor.getFromConfig(ConfigParametersTCP::TYPE))
    , socketDomain(sourceDescriptor.getFromConfig(ConfigParametersTCP::DOMAIN))
    , decideMessageSize(sourceDescriptor.getFromConfig(ConfigParametersTCP::DECIDED_MESSAGE_SIZE))
    , tupleSeparator(sourceDescriptor.getFromConfig(ConfigParametersTCP::SEPARATOR))
    , socketBufferSize(sourceDescriptor.getFromConfig(ConfigParametersTCP::SOCKET_BUFFER_SIZE))
    , bytesUsedForSocketBufferSizeTransfer(sourceDescriptor.getFromConfig(ConfigParametersTCP::SOCKET_BUFFER_TRANSFER_SIZE))
    , flushIntervalInMs(sourceDescriptor.getFromConfig(ConfigParametersTCP::FLUSH_INTERVAL_MS))
{
    /// init physical types
    std::vector<std::string> schemaKeys;
    const DefaultPhysicalTypeFactory defaultPhysicalTypeFactory{};

    NES_TRACE("SourceTCP::SourceTCP: Init SourceTCP.");
}

std::ostream& SourceTCP::toString(std::ostream& str) const
{
    str << "\nSourceTcp(";
    str << "\n  generated tuples: " << this->generatedTuples;
    str << "\n  generated buffers: " << this->generatedBuffers;
    str << "\n  connection: " << this->connection;
    str << "\n  timeout: " << TCP_SOCKET_DEFAULT_TIMEOUT.count() << " microseconds";
    str << "\n  inputFormat: " << magic_enum::enum_name(inputFormat);
    str << "\n  socketHost: " << socketHost;
    str << "\n  socketPort: " << socketPort;
    str << "\n  socketType: " << socketType;
    str << "\n  socketDomain: " << socketDomain;
    str << "\n  decideMessageSize: " << magic_enum::enum_name(decideMessageSize);
    str << "\n  tupleSeparator: " << tupleSeparator;
    str << "\n  socketBufferSize: " << socketBufferSize;
    str << "\n  bytesUsedForSocketBufferSizeTransfer" << bytesUsedForSocketBufferSizeTransfer;
    str << "\n  flushIntervalInMs" << flushIntervalInMs;
    str << ")\n";
    return str;
}

void SourceTCP::open()
{
    NES_TRACE("SourceTCP::open: Trying to create socket and connect.");

    addrinfo hints;
    addrinfo* result;

    hints.ai_family = socketDomain;
    hints.ai_socktype = socketType;
    hints.ai_flags = 0; /// use default behavior
    hints.ai_protocol
        = 0; /// specifying 0 in this field indicates that socket addresses with any protocol can be returned by getaddrinfo() ;

    const auto errorCode = getaddrinfo(socketHost.c_str(), socketPort.c_str(), &hints, &result);
    if (errorCode != 0)
    {
        /// #72: use correct error type
        NES_THROW_RUNTIME_ERROR("SourceTCP::open: getaddrinfo failed. Error: " << gai_strerror(errorCode));
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

        constexpr static timeval timeout{0, TCP_SOCKET_DEFAULT_TIMEOUT.count()};
        setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
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
        /// #72: use correct error type
        NES_THROW_RUNTIME_ERROR("SourceTCP::open: Could not connect to " << socketHost << ":" << socketPort);
    }

    NES_TRACE("SourceTCP::open: Connected to server.");
}

bool SourceTCP::fillTupleBuffer(
    NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider& bufferManager, const ParserCSV& parserCSV)
{
    std::stringstream ss;
    ss << this;
    NES_DEBUG("SourceTCP  {}: receiveData ", ss.str());
    NES_DEBUG("SourceTCP buffer allocated ");
    try
    {
        while (fillBuffer(tupleBuffer, bufferManager, parserCSV))
        {
            /// Fill the buffer until EoS reached or the number of tuples in the buffer is not equals to 0.
        };
        return tupleBuffer.getNumberOfTuples() > 0;
    }
    catch (const std::exception& e)
    {
        NES_ERROR("SourceTCP::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw e;
    }
}

std::pair<bool, size_t> sizeUntilSearchToken(std::span<const char> data, char token)
{
    auto result = std::find_if(data.begin(), data.end(), [token](auto& c) { return c == token; });

    return {result != data.end(), std::distance(data.begin(), result)};
}

size_t asciiBufferSize(const std::span<const char> data)
{
    return std::stoll(std::string(data.begin(), data.end()));
}

size_t binaryBufferSize(std::span<const char> data)
{
    static_assert(std::endian::native == std::endian::little, "Only implemented for little endian");
    NES_ASSERT(data.size() <= sizeof(size_t), "Not implemented for " << data.size() << "socket buffer size");

    size_t result = 0;
    std::copy(data.begin(), data.end(), reinterpret_cast<char*>(&result));
    return result;
}

size_t SourceTCP::parseBufferSize(const std::span<const char> data) const
{
    return asciiBufferSize(data);
}

bool SourceTCP::fillBuffer(
    NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider& bufferManager, const ParserCSV& parserCSV)
{
    /// determine how many tuples fit into the buffer
    const auto tupleSize = parserCSV.getSizeOfSchemaInBytes();
    const auto tuplesThisPass = tupleBuffer.getBufferSize() / tupleSize;
    NES_DEBUG("SourceTCP::fillBuffer: Fill buffer with #tuples= {}  of size= {}", tuplesThisPass, tupleSize);
    /// init tuple count for buffer
    uint64_t tupleCount = 0;
    /// init timer for flush interval
    auto flushIntervalTimerStart = std::chrono::system_clock::now();
    /// init flush interval value
    bool flushIntervalPassed = false;
    /// receive data until tupleBuffer capacity reached or flushIntervalPassed
    bool isEoS = false;

    while (tupleCount < tuplesThisPass && !flushIntervalPassed)
    {
        /// if circular buffer is not full obtain data from socket
        if (!circularBuffer.full())
        {
            auto writer = circularBuffer.write();

            auto bufferSizeReceived = read(sockfd, writer.data(), writer.size());
            if (bufferSizeReceived == -1)
            {
                /// if read method returned -1 an error occurred during read.
                NES_ERROR("SourceTCP::fillBuffer: an error occurred while reading from socket. Error: {}", strerror(errno));
                isEoS = true;
                break;
            }
            if (bufferSizeReceived == 0)
            {
                NES_TRACE("SourceTCP::fillBuffer: No data received from {}:{}.", socketHost, socketPort);
            }

            writer.consume(bufferSizeReceived);
            if (bufferSizeReceived == 0 && circularBuffer.empty())
            {
                NES_INFO("TCP Source detected EoS");
                isEoS = true;
                break;
            }
        }

        if (!circularBuffer.empty())
        {
            auto reader = circularBuffer.read();
            auto tupleData = std::span<const char>{};
            NES_ASSERT(tupleData.empty(), "not empty");
            /// Every protocol returns a view into the tuple (or Buffer for Binary) memory in tupleData;
            /// switch case depends on the message receiving that was chosen when creating the source. Three choices are available:
            switch (decideMessageSize)
            {
                /// The user inputted a tuple separator that indicates the end of a tuple. We're going to search for that
                /// tuple seperator and assume that all data until then belongs to the current tuple
                case Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR: {
                    /// search the circularBuffer until Tuple seperator is found to obtain size of tuple
                    auto [foundSeparator, inputTupleSize] = sizeUntilSearchToken(reader, tupleSeparator);

                    if (!foundSeparator)
                    {
                        NES_TRACE("Separator not found");
                        break;
                    }

                    tupleData = reader.consume(inputTupleSize);
                    reader.consume(sizeof(tupleSeparator));
                    break;
                }
                /// The user inputted a fixed buffer size.
                case Configurations::TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE: {
                    auto inputTupleSize = socketBufferSize;

                    if (reader.size() < inputTupleSize)
                    {
                        break;
                    }

                    tupleData = reader.consume(inputTupleSize);
                    break;
                }
                /// Before each message, the server uses a fixed number of bytes (bytesUsedForSocketBufferSizeTransfer)
                /// to indicate the size of the next tuple.
                case Configurations::TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET: {
                    /// Tuple (or Buffer for Binary) Size preceds the actual data.
                    /// Peek BytesUserForSocketBufferSize so if the buffer contains not enough bytes the next iteration does not
                    /// loose the tuple size information.
                    auto bufferSizeSize = bytesUsedForSocketBufferSizeTransfer;
                    if (reader.size() < bufferSizeSize)
                    {
                        break;
                    }

                    auto bufferSizeMemory = std::span<const char>(reader).subspan(0, bufferSizeSize);
                    auto size = parseBufferSize(bufferSizeMemory);
                    NES_TRACE("tuple size from socket: {}", size);
                    if (reader.size() < bufferSizeSize + size)
                    {
                        NES_TRACE("Waiting for data current {}", reader.size() - bufferSizeSize);
                        break;
                    }

                    /// Consume the size and data iff a complete tuple (or Buffer) is available.
                    reader.consume(bufferSizeSize);
                    tupleData = reader.consume(size);
                    break;
                }
            }

            /// if we were able to obtain a complete tuple from the circular buffer, we are going to forward it ot the appropriate parser
            if (!tupleData.empty())
            {
                std::string_view buf(tupleData.data(), tupleData.size());
                NES_TRACE("SOURCETCP::fillBuffer: Client consume message: '{}'.", buf);
                parserCSV.writeInputTupleToTupleBuffer(buf, tupleCount, tupleBuffer, bufferManager);
                tupleCount++;
            }
        }
        /// If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
        /// and writing data exceeds the user defined limit (bufferFlushIntervalMs).
        /// If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
        if ((flushIntervalInMs > 0
             && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - flushIntervalTimerStart).count()
                 >= flushIntervalInMs))
        {
            NES_DEBUG("SourceTCP::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
    }
    tupleBuffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    /// Return false, if there are tuples in the buffer, or the EoS was reached.
    return tupleBuffer.getNumberOfTuples() == 0 && !isEoS;
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SourceTCP::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), NAME);
}

void SourceTCP::close()
{
    NES_TRACE("SourceTCP::close: trying to close connection.");
    if (connection >= 0)
    {
        ::close(connection);
        ::close(sockfd);
        NES_TRACE("SourceTCP::close: connection closed.");
    }
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SourceGeneratedRegistrarValidation::RegisterSourceValidationTCP(std::unordered_map<std::string, std::string>&& sourceConfig)
{
    return SourceTCP::validateAndFormat(std::move(sourceConfig));
}

std::unique_ptr<Source> SourceGeneratedRegistrar::RegisterSourceTCP(const SourceDescriptor& sourceDescriptor)
{
    return std::make_unique<SourceTCP>(sourceDescriptor);
}

}
