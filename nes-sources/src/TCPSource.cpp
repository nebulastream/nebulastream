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
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Sources/Registry/GeneratedSourceRegistrar.hpp>
#include <Sources/Registry/SourceRegistry.hpp>
#include <Sources/TCPSource.hpp>
#include <sys/socket.h> /// For socket functions
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::Sources
{

void GeneratedSourceRegistrar::RegisterTCPSource(SourceRegistry& registry)
{
    const auto constructorFunc = [](const Schema& schema, std::unique_ptr<SourceDescriptor>&& sourceDescriptor) -> std::unique_ptr<Source>
    { return std::make_unique<TCPSource>(schema, std::move(sourceDescriptor)); };
    registry.registerPlugin((TCPSource::PLUGIN_NAME), constructorFunc);
}

TCPSource::TCPSource(const Schema& schema, std::unique_ptr<SourceDescriptor>&& sourceDescriptor)
    : tupleSize(schema.getSchemaSizeInBytes()), circularBuffer(getpagesize() * 2)
{
    auto tcpSourceType = dynamic_cast<TCPSourceDescriptor*>(sourceDescriptor.get())->getSourceConfig();
    this->inputFormat = tcpSourceType->getInputFormat()->getValue();
    this->socketHost = tcpSourceType->getSocketHost()->getValue();
    this->socketPort = std::to_string(static_cast<int>(tcpSourceType->getSocketPort()->getValue()));
    this->socketType = static_cast<int>(tcpSourceType->getSocketType()->getValue());
    this->socketDomain = static_cast<int>(tcpSourceType->getSocketDomain()->getValue());
    this->decideMessageSize = tcpSourceType->getDecideMessageSize()->getValue();
    this->tupleSeparator = tcpSourceType->getTupleSeparator()->getValue();
    this->socketBufferSize = static_cast<size_t>(tcpSourceType->getSocketBufferSize()->getValue());
    this->bytesUsedForSocketBufferSizeTransfer = static_cast<size_t>(tcpSourceType->getBytesUsedForSocketBufferSizeTransfer()->getValue());
    this->flushIntervalInMs = tcpSourceType->getFlushIntervalMS()->getValue();

    /// init physical types
    std::vector<std::string> schemaKeys;
    const DefaultPhysicalTypeFactory defaultPhysicalTypeFactory{};

    /// Extracting the schema keys in order to parse incoming data correctly (e.g. use as keys for JSON objects)
    /// Also, extracting the field types in order to parse and cast the values of incoming data to the correct types
    for (const auto& field : schema.fields)
    {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
        auto fieldName = field->getName();
        NES_TRACE("TCPSOURCE:: Schema keys are:  {}", fieldName);
        schemaKeys.push_back(fieldName.substr(fieldName.find('$') + 1, fieldName.size()));
    }

    switch (inputFormat)
    {
        case Configurations::InputFormat::CSV:
            inputParser = std::make_unique<CSVParser>(schema.getSize(), physicalTypes, ",");
            break;
        default:
            NES_ERROR("InputFormat not supported.")
            NES_NOT_IMPLEMENTED();
    }

    NES_TRACE("TCPSource::TCPSource: Init TCPSource.");
}

std::string TCPSource::toString() const
{
    std::stringstream ss;
    ss << "TCPSOURCE(";
    ss << "Tuplesize:" << this->tupleSize;
    ss << "Generated tuples: " << this->generatedTuples;
    ss << "Generated buffers: " << this->generatedBuffers;
    ss << "Connection: " << this->connection;
    ss << "Timeout: " << TCP_SOCKET_DEFAULT_TIMEOUT.count() << " microseconds";
    ss << "InputFormat: " << magic_enum::enum_name(inputFormat);
    ss << "SocketHost: " << socketHost;
    ss << "SocketPort: " << socketPort;
    ss << "SocketType: " << socketType;
    ss << "SocketDomain: " << socketDomain;
    ss << "DecideMessageSize: " << magic_enum::enum_name(decideMessageSize);
    ss << "TupleSeparator: " << tupleSeparator;
    ss << "SocketBufferSize: " << socketBufferSize;
    ss << "BytesUsedForSocketBufferSizeTransfer" << bytesUsedForSocketBufferSizeTransfer;
    ss << "FlushIntervalInMs" << flushIntervalInMs;
    ss << ")";
    return ss.str();
}

void TCPSource::open()
{
    NES_TRACE("TCPSource::open: Trying to create socket and connect.");

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
        NES_THROW_RUNTIME_ERROR("TCPSource::open: getaddrinfo failed. Error: " << gai_strerror(errorCode));
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
        NES_THROW_RUNTIME_ERROR("TCPSource::open: Could not connect to " << socketHost << ":" << socketPort);
    }

    NES_TRACE("TCPSource::open: Connected to server.");
}

bool TCPSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, Memory::AbstractBufferProvider& bufferManager, const Schema& schema)
{
    NES_DEBUG("TCPSource  {}: receiveData ", this->toString());
    NES_DEBUG("TCPSource buffer allocated ");
    try
    {
        while (fillBuffer(tupleBuffer, bufferManager, schema))
        {
            /// Fill the buffer until EoS reached or the number of tuples in the buffer is not equals to 0.
        };
        return tupleBuffer.getNumberOfTuples() > 0;
    }
    catch (const std::exception& e)
    {
        NES_ERROR("TCPSource::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
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

size_t TCPSource::parseBufferSize(std::span<const char> data) const
{
    if (inputFormat == Configurations::InputFormat::NES_BINARY)
    {
        return binaryBufferSize(data);
    }

    return asciiBufferSize(data);
}

bool TCPSource::fillBuffer(NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider& bufferManager, const Schema& schema)
{
    /// determine how many tuples fit into the buffer
    const auto tuplesThisPass = tupleBuffer.getBufferSize() / tupleSize;
    NES_DEBUG("TCPSource::fillBuffer: Fill buffer with #tuples= {}  of size= {}", tuplesThisPass, tupleSize);
    /// init tuple count for buffer
    uint64_t tupleCount = 0;
    /// init timer for flush interval
    auto flushIntervalTimerStart = std::chrono::system_clock::now();
    /// init flush interval value
    bool flushIntervalPassed = false;
    /// receive data until tupleBuffer capacity reached or flushIntervalPassed
    bool isEoS = false;

    /// Todo #72: remove TestTupleBuffer creation.
    /// We need to create a TestTupleBuffer here, because if we do it after calling 'writeInputTupleToTupleBuffer' we repeatedly create a
    /// TestTupleBuffer for the same TupleBuffer.
    auto testTupleBuffer = NES::Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schema);
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
                NES_ERROR("TCPSource::fillBuffer: an error occurred while reading from socket. Error: {}", strerror(errno));
                isEoS = true;
                break;
            }
            if (bufferSizeReceived == 0)
            {
                NES_TRACE("TCPSource::fillBuffer: No data received from {}:{}.", socketHost, socketPort);
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
                NES_TRACE("TCPSOURCE::fillBuffer: Client consume message: '{}'.", buf);
                inputParser->writeInputTupleToTupleBuffer(buf, tupleCount, testTupleBuffer, schema, bufferManager);
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
            NES_DEBUG("TCPSource::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
    }
    testTupleBuffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    /// Return false, if there are tuples in the buffer, or the EoS was reached.
    return testTupleBuffer.getNumberOfTuples() == 0 && !isEoS;
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

SourceType TCPSource::getType() const
{
    return SourceType::TCP_SOURCE;
}
}
