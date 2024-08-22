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

#include <charconv>
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
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Sources/Registry/GeneratedSourceRegistrar.hpp>
#include <Sources/Registry/SourceRegistry.hpp>
#include <Sources/TCPSource.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <sys/socket.h> /// For socket functions
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::Sources
{

void GeneratedSourceRegistrar::RegisterTCPSource(SourceRegistry& registry)
{
    const auto constructorFunc = []() -> std::unique_ptr<Source> { return std::make_unique<TCPSource>(); };
    registry.registerPlugin((TCPSource::PLUGIN_NAME), constructorFunc);
}

void TCPSource::configure(SchemaPtr schema, TCPSourceTypePtr tcpSourceType)
{
    this->schema = std::move(schema);
    this->sourceConfig = std::move(tcpSourceType);
    this->tupleSize = this->schema->getSchemaSizeInBytes();

    /// init physical types
    std::vector<std::string> schemaKeys;
    const DefaultPhysicalTypeFactory defaultPhysicalTypeFactory{};

    /// Extracting the schema keys in order to parse incoming data correctly (e.g. use as keys for JSON objects)
    /// Also, extracting the field types in order to parse and cast the values of incoming data to the correct types
    for (const auto& field : this->schema->fields)
    {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
        auto fieldName = field->getName();
        NES_TRACE("TCPSOURCE:: Schema keys are:  {}", fieldName);
        schemaKeys.push_back(fieldName.substr(fieldName.find('$') + 1, fieldName.size()));
    }

    switch (sourceConfig->getInputFormat()->getValue())
    {
        case Configurations::InputFormat::CSV:
            inputParser = std::make_unique<CSVParser>(this->schema->getSize(), physicalTypes, ",");
            break;
        default:
            NES_ERROR("InputFormat not supported.")
            NES_NOT_IMPLEMENTED();
    }

    NES_TRACE("TCPSource::TCPSource: Init TCPSource.");
}
TCPSource::TCPSource(SchemaPtr schema, TCPSourceTypePtr tcpSourceType)
    : tupleSize(schema->getSchemaSizeInBytes())
    , sourceConfig(std::move(tcpSourceType))
    , timeout(TCP_SOCKET_DEFAULT_TIMEOUT)
    , circularBuffer(getpagesize() * 2)
    , schema(schema)
{
    /// init physical types
    std::vector<std::string> schemaKeys;
    const DefaultPhysicalTypeFactory defaultPhysicalTypeFactory{};

    /// Extracting the schema keys in order to parse incoming data correctly (e.g. use as keys for JSON objects)
    /// Also, extracting the field types in order to parse and cast the values of incoming data to the correct types
    for (const auto& field : schema->fields)
    {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
        auto fieldName = field->getName();
        NES_TRACE("TCPSOURCE:: Schema keys are:  {}", fieldName);
        schemaKeys.push_back(fieldName.substr(fieldName.find('$') + 1, fieldName.size()));
    }

    switch (sourceConfig->getInputFormat()->getValue())
    {
        case Configurations::InputFormat::CSV:
            inputParser = std::make_unique<CSVParser>(schema->getSize(), physicalTypes, ",");
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
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << sourceConfig->toString();
    return ss.str();
}

void TCPSource::open()
{
    NES_TRACE("TCPSource::open: Trying to create socket and connect.");

    addrinfo hints;
    addrinfo* result;

    hints.ai_family = static_cast<int>(sourceConfig->getSocketDomain()->getValue());
    hints.ai_socktype = static_cast<int>(sourceConfig->getSocketType()->getValue());
    hints.ai_flags = 0; /// use default behavior
    hints.ai_protocol = 0; /// specifying 0 in this field indicates that socket addresses with any protocol can be returned by getaddrinfo()

    auto host = sourceConfig->getSocketHost()->getValue();
    auto port = std::to_string(sourceConfig->getSocketPort()->getValue());

    const auto errorCode = getaddrinfo(host.c_str(), port.c_str(), &hints, &result);
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

        setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
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
        NES_THROW_RUNTIME_ERROR("TCPSource::open: Could not connect to " << host << ":" << port);
    }

    NES_TRACE("TCPSource::open: Connected to server.");
}

bool TCPSource::fillTupleBuffer(
    NES::Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer, const std::shared_ptr<NES::Runtime::AbstractBufferProvider>& bufferManager)
{
    NES_DEBUG("TCPSource  {}: receiveData ", this->toString());
    NES_DEBUG("TCPSource buffer allocated ");
    try
    {
        while (fillBuffer(tupleBuffer, bufferManager))
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

std::pair<bool, size_t> sizeUntilSearchToken(SPAN_TYPE<const char> data, char token)
{
    auto result = std::find_if(data.begin(), data.end(), [token](auto& c) { return c == token; });

    return {result != data.end(), std::distance(data.begin(), result)};
}

size_t asciiBufferSize(const SPAN_TYPE<const char> data)
{
    return std::stoll(std::string(data.begin(), data.end()));
}

size_t binaryBufferSize(SPAN_TYPE<const char> data)
{
    static_assert(std::endian::native == std::endian::little, "Only implemented for little endian");
    NES_ASSERT(data.size() <= sizeof(size_t), "Not implemented for " << data.size() << "socket buffer size");

    size_t result = 0;
    std::copy(data.begin(), data.end(), reinterpret_cast<char*>(&result));
    return result;
}

size_t TCPSource::parseBufferSize(SPAN_TYPE<const char> data) const
{
    if (sourceConfig->getInputFormat()->getValue() == Configurations::InputFormat::NES_BINARY)
    {
        return binaryBufferSize(data);
    }

    return asciiBufferSize(data);
}

bool TCPSource::fillBuffer(
    NES::Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer, const std::shared_ptr<NES::Runtime::AbstractBufferProvider>& bufferManager)
{
    /// determine how many tuples fit into the buffer
    tuplesThisPass = tupleBuffer.getCapacity();
    NES_DEBUG("TCPSource::fillBuffer: Fill buffer with #tuples= {}  of size= {}", tuplesThisPass, tupleSize);
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
                NES_ERROR("TCPSource::fillBuffer: an error occurred while reading from socket. Error: {}", strerror(errno));
                isEoS = true;
                break;
            }
            if (bufferSizeReceived == 0)
            {
                NES_TRACE(
                    "TCPSource::fillBuffer: No data received from {}:{}.",
                    sourceConfig->getSocketHost()->getValue(),
                    sourceConfig->getSocketPort()->getValue());
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
            auto tupleData = SPAN_TYPE<const char>{};
            NES_ASSERT(tupleData.empty(), "not empty");
            /// Every protocol returns a view into the tuple (or Buffer for Binary) memory in tupleData;
            /// switch case depends on the message receiving that was chosen when creating the source. Three choices are available:
            switch (sourceConfig->getDecideMessageSize()->getValue())
            {
                /// The user inputted a tuple separator that indicates the end of a tuple. We're going to search for that
                /// tuple seperator and assume that all data until then belongs to the current tuple
                case Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR: {
                    /// search the circularBuffer until Tuple seperator is found to obtain size of tuple
                    auto [foundSeparator, inputTupleSize]
                        = sizeUntilSearchToken(reader, this->sourceConfig->getTupleSeparator()->getValue());

                    if (!foundSeparator)
                    {
                        NES_TRACE("Separator not found");
                        break;
                    }

                    tupleData = reader.consume(inputTupleSize);
                    reader.consume(sizeof(this->sourceConfig->getTupleSeparator()->getValue()));
                    break;
                }
                /// The user inputted a fixed buffer size.
                case Configurations::TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE: {
                    auto inputTupleSize = sourceConfig->getSocketBufferSize()->getValue();

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
                    auto bufferSizeSize = sourceConfig->getBytesUsedForSocketBufferSizeTransfer()->getValue();
                    if (reader.size() < bufferSizeSize)
                    {
                        break;
                    }

                    auto bufferSizeMemory = SPAN_TYPE<const char>(reader).subspan(0, bufferSizeSize);
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
                inputParser->writeInputTupleToTupleBuffer(buf, tupleCount, tupleBuffer, schema, bufferManager);
                tupleCount++;
            }
        }
        /// If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
        /// and writing data exceeds the user defined limit (bufferFlushIntervalMs).
        /// If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
        if ((sourceConfig->getFlushIntervalMS()->getValue() > 0
             && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - flushIntervalTimerStart).count()
                 >= sourceConfig->getFlushIntervalMS()->getValue()))
        {
            NES_DEBUG("TCPSource::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
    }
    tupleBuffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    /// Return false, if there are tuples in the buffer, or the EoS was reached.
    return tupleBuffer.getNumberOfTuples() == 0 && !isEoS;
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

const TCPSourceTypePtr& TCPSource::getSourceConfig() const
{
    return sourceConfig;
}

}
