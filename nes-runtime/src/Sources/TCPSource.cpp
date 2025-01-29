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

#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Sources/Parsers/NESBinaryParser.hpp>
#include <Sources/PersistentSourceProperties/PersistentTCPSourceProperties.hpp>
#include <Sources/TCPSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <arpa/inet.h>
#include <charconv>
#include <chrono>
#include <cstring>
#include <errno.h>     // For socket error
#include <netinet/in.h>// For sockaddr_in
#include <sstream>
#include <string>
#include <sys/socket.h>// For socket functions
#include <unistd.h>    // For read
#include <utility>
#include <vector>

namespace NES {

TCPSource::TCPSource(SchemaPtr schema,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     const TCPSourceTypePtr& tcpSourceType,
                     OperatorId operatorId,
                     OriginId originId,
                     StatisticId statisticId,
                     size_t numSourceLocalBuffers,
                     GatheringMode gatheringMode,
                     const std::string& physicalSourceName,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 statisticId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 physicalSourceName,
                 tcpSourceType->getPersistentTcpSource()->getValue(),
                 false,
                 std::move(executableSuccessors)),
      tupleSize(schema->getSchemaSizeInBytes()), sourceConfig(std::move(tcpSourceType)) {

    //init physical types
    std::vector<std::string> schemaKeys;
    std::string fieldName;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();

    //Extracting the schema keys in order to parse incoming data correctly (e.g. use as keys for JSON objects)
    //Also, extracting the field types in order to parse and cast the values of incoming data to the correct types
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
        fieldName = field->getName();
        NES_TRACE("Schema keys are:  {}", fieldName);
        schemaKeys.push_back(fieldName.substr(fieldName.find('$') + 1, fieldName.size()));
    }

    switch (sourceConfig->getInputFormat()->getValue()) {
        case Configurations::InputFormat::JSON:
            inputParser = std::make_unique<JSONParser>(schema->getSize(), schemaKeys, physicalTypes);
            break;
        case Configurations::InputFormat::CSV:
            inputParser = std::make_unique<CSVParser>(schema->getSize(), physicalTypes, ",");
            break;
        case Configurations::InputFormat::NES_BINARY:
            inputParser =
                std::make_unique<NESBinaryParser>(this->schema, this->sourceConfig->addIngestionTimeEnabled()->getValue());
            break;
    }

    NES_TRACE("Init TCPSource.");
}

std::string TCPSource::toString() const {
    std::stringstream ss;
    ss << "TCPSOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << sourceConfig->toString();
    return ss.str();
}

void TCPSource::open() {
    DataSource::open();
    if (connection < 0) {
        connection = -1;
        NES_THROW_RUNTIME_ERROR("Connection with server failed. Error: " << strerror(errno));
    }
    NES_TRACE("Connected to server.");
}

std::optional<Runtime::TupleBuffer> TCPSource::receiveData() {
    NES_DEBUG("TCPSource with id {}: receiveData ", this->toString());
    auto tupleBuffer = allocateBuffer();
    NES_DEBUG("TCPSource buffer allocated ");
    try {
        do {
            if (!running) {
                return std::nullopt;
            }
            fillBuffer(tupleBuffer);
        } while (tupleBuffer.getNumberOfTuples() == 0);
    } catch (const std::exception& e) {
        NES_ERROR("Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw e;
    }
    return tupleBuffer.getBuffer();
}

std::pair<bool, size_t> sizeUntilSearchToken(SPAN_TYPE<const char> data, char token) {
    auto result = std::find_if(data.begin(), data.end(), [token](auto& c) {
        return c == token;
    });

    return {result != data.end(), std::distance(data.begin(), result)};
}

size_t asciiBufferSize(const SPAN_TYPE<const char> data) { return std::stoll(std::string(data.begin(), data.end())); }

size_t binaryBufferSize(SPAN_TYPE<const char> data) {
    static_assert(std::endian::native == std::endian::little, "Only implemented for little endian");
    NES_ASSERT(data.size() <= sizeof(size_t), "Not implemented for " << data.size() << "socket buffer size");

    size_t result = 0;
    std::copy(data.begin(), data.end(), reinterpret_cast<char*>(&result));
    return result;
}

size_t TCPSource::parseBufferSize(SPAN_TYPE<const char> data) const {
    if (sourceConfig->getInputFormat()->getValue() == Configurations::InputFormat::NES_BINARY) {
        return binaryBufferSize(data);
    }
    return asciiBufferSize(data);
}

bool TCPSource::fillBuffer(Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer) {
    // determine how many tuples fit into the buffer
    tuplesThisPass = tupleBuffer.getCapacity();
    NES_DEBUG("Fill buffer with #tuplesThisPass={} of size={}, operatorId={}", tuplesThisPass, tupleSize, operatorId);
    //init tuple count for buffer
    uint64_t tupleCount = 0;
    //init timer for flush interval
    auto flushIntervalTimerStart = std::chrono::system_clock::now();
    //init flush interval value
    bool flushIntervalPassed = false;
    //receive data until tupleBuffer capacity reached or flushIntervalPassed
    NES_DEBUG("operatorId={}, circularBufferSize={}", operatorId, circularBuffer->size());
    while (tupleCount < tuplesThisPass && !flushIntervalPassed) {
        //if circular buffer is not full obtain data from socket
        if (!circularBuffer->full()) {
            auto writer = circularBuffer->write();
            auto bufferSizeReceived = read(sockfd, writer.data(), writer.size());
            //if read method returned -1 an error occurred during read.
            if (bufferSizeReceived == -1) {
                NES_WARNING("An error or timeout occurred while reading from socket. Error: {}", strerror(errno));
            } else if (bufferSizeReceived == 0 && circularBuffer->empty()) {
                NES_WARNING("TCP Source detected EoS");
                this->running.exchange(false);
                break;
            } else {
                writer.consume(bufferSizeReceived);
            }
        }

        if (!circularBuffer->empty()) {
            auto reader = circularBuffer->read();
            auto tupleData = SPAN_TYPE<const char>{};
            NES_ASSERT(tupleData.empty(), "not empty");
            // Every protocol returns a view into the tuple (or Buffer for Binary) memory in tupleData;
            // switch case depends on the message receiving that was chosen when creating the source. Three choices are available:
            switch (sourceConfig->getDecideMessageSize()->getValue()) {
                // The user inputted a tuple separator that indicates the end of a tuple. We're going to search for that
                // tuple seperator and assume that all data until then belongs to the current tuple
                case Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR: {
                    // search the circularBuffer until Tuple seperator is found to obtain size of tuple
                    auto [foundSeparator, inputTupleSize] =
                        sizeUntilSearchToken(reader, this->sourceConfig->getTupleSeparator()->getValue());

                    if (!foundSeparator) {
                        NES_TRACE("Separator not found");
                        break;
                    }

                    tupleData = reader.consume(inputTupleSize);
                    reader.consume(sizeof(this->sourceConfig->getTupleSeparator()->getValue()));
                    break;
                }
                // The user inputted a fixed buffer size.
                case Configurations::TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE: {
                    auto inputTupleSize = sourceConfig->getSocketBufferSize()->getValue();
                    if (reader.size() < inputTupleSize) {
                        break;
                    }
                    tupleData = reader.consume(inputTupleSize);
                    break;
                }
                // Before each message, the server uses a fixed number of bytes (bytesUsedForSocketBufferSizeTransfer)
                // to indicate the size of the next tuple.
                case Configurations::TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET: {
                    // Tuple (or Buffer for Binary) Size preceds the actual data.
                    // Peek BytesUserForSocketBufferSize so if the buffer contains not enough bytes the next iteration does not
                    // loose the tuple size information.
                    auto bufferSizeSize = sourceConfig->getBytesUsedForSocketBufferSizeTransfer()->getValue();
                    if (reader.size() < bufferSizeSize) {
                        break;
                    }

                    auto bufferSizeMemory = SPAN_TYPE<const char>(reader).subspan(0, bufferSizeSize);
                    auto size = parseBufferSize(bufferSizeMemory);
                    NES_TRACE("tuple size from socket: {}", size);
                    if (reader.size() < bufferSizeSize + size) {
                        NES_TRACE("Waiting for data current {}", reader.size() - bufferSizeSize);
                        break;
                    }

                    // Consume the size and data iff a complete tuple (or Buffer) is available.
                    reader.consume(bufferSizeSize);
                    tupleData = reader.consume(size);
                    break;
                }
            }

            //if we were able to obtain a complete tuple from the circular buffer, we are going to forward it ot the appropriate parser
            if (!tupleData.empty()) {
                std::string_view buf(tupleData.data(), tupleData.size());
                if (sourceConfig->getInputFormat()->getValue() == Configurations::InputFormat::NES_BINARY) {
                    //Note: Only one pass is done of the write method as it performs memcopy of the received buffer to the tuple buffer.
                    inputParser->writeInputTupleToTupleBuffer(buf, tupleCount, tupleBuffer, schema, localBufferManager);
                    tupleCount = tupleBuffer.getNumberOfTuples();
                    // break the while loop as otherwise we will overwrite the tuple buffer.
                    flushIntervalPassed = true;
                } else {
                    NES_TRACE("Client consume message: '{}'.", buf);
                    inputParser->writeInputTupleToTupleBuffer(buf, tupleCount, tupleBuffer, schema, localBufferManager);
                    tupleCount++;
                }
            }
        }
        // If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
        // and writing data exceeds the user defined limit (bufferFlushIntervalMs).
        // If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
        if ((sourceConfig->getFlushIntervalMS()->getValue() > 0
             && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - flushIntervalTimerStart)
                     .count()
                 >= sourceConfig->getFlushIntervalMS()->getValue())) {
            NES_DEBUG("Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
    }
    tupleBuffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    return true;
}

void TCPSource::close() {
    NES_TRACE("Trying to close connection.");
    DataSource::close();
    // close socket if persistent TCP source is not configured or persistent tcp source is configured but hard stop is requested
    bool closeSocket = !persistentSource || (persistentSource && wasGracefullyStopped == Runtime::QueryTerminationType::HardStop);
    if (connection >= 0 && closeSocket) {
        ::close(sockfd);
        NES_ERROR("connection closed.");
    } else {
        NES_ERROR("Skipped connection closer.");
    }
}

SourceType TCPSource::getType() const { return SourceType::TCP_SOURCE; }

const TCPSourceTypePtr& TCPSource::getSourceConfig() const { return sourceConfig; }

void TCPSource::createOrLoadPersistedProperties() {
    if (persistentSource) {
        NES_TRACE("Trying to load persistent TCP socket properties.");
        auto lockedPersistentSourceProperties = this->queryManager->persistentSourceProperties.ulock();
        if (lockedPersistentSourceProperties->contains(persistentSourceKey)) {
            NES_TRACE("Found and loading persistent TCP socket properties.");
            auto wLocked = lockedPersistentSourceProperties.moveFromUpgradeToWrite();
            auto persistentProperties = (*wLocked)[persistentSourceKey];
            auto persistentTPSourceProperties = std::static_pointer_cast<PersistentTCPSourceProperties>(persistentProperties);
            sockfd = persistentTPSourceProperties->socketFd;
            circularBuffer = persistentTPSourceProperties->circularBuffer;
            connection = 0;
            return;
        }
    }

    NES_TRACE("Trying to create socket.");
    circularBuffer = std::make_shared<MMapCircularBuffer>(getpagesize() * 2);
    sockfd = socket(sourceConfig->getSocketDomain()->getValue(), sourceConfig->getSocketType()->getValue(), 0);
    NES_TRACE("Socket created with  {}", sockfd);
    if (sockfd < 0) {
        NES_ERROR("Failed to create socket. Error: {}", strerror(errno));
        connection = -1;
        return;
    }

    struct timeval timeout;
    timeout.tv_sec = 0;          // timeout in seconds
    timeout.tv_usec = 100 * 1000;// timeout, 100ms converted to microseconds
    // Set the receive timeout option
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        NES_ERROR("setsockopt failed");
    }

    NES_TRACE("Created socket");
    struct sockaddr_in servaddr;
    servaddr.sin_family = sourceConfig->getSocketDomain()->getValue();
    servaddr.sin_addr.s_addr = inet_addr(sourceConfig->getSocketHost()->getValue().c_str());
    servaddr.sin_port =
        htons(sourceConfig->getSocketPort()->getValue());// htons is necessary to convert a number to network byte order

    if (connection < 0) {
        NES_TRACE("Try connecting to server: {}:{}",
                  sourceConfig->getSocketHost()->getValue(),
                  sourceConfig->getSocketPort()->getValue());
        connection = connect(sockfd, (struct sockaddr*) &servaddr, sizeof(servaddr));
    }
}

void TCPSource::storePersistedProperties() {
    if (persistentSource) {
        auto lockedPersistentSourceProperties = this->queryManager->persistentSourceProperties.ulock();
        if (!lockedPersistentSourceProperties->contains(persistentSourceKey)) {
            NES_WARNING("Storing tcp source persistent properties.");
            auto wLocked = lockedPersistentSourceProperties.moveFromUpgradeToWrite();
            auto persistentTCPSourceProperties = std::make_shared<PersistentTCPSourceProperties>(sockfd, circularBuffer);
            (*wLocked)[persistentSourceKey] = persistentTCPSourceProperties;
        }
    }
}

void TCPSource::clearPersistedProperties() {
    if (persistentSource) {
        // close socket if persistent TCP source is not configured or persistent tcp source is configured but hard stop is requested
        if (wasGracefullyStopped == Runtime::QueryTerminationType::HardStop) {
            NES_WARNING("Deleting tcp source persistent properties.");
            auto lockedPersistentSourceProperties = queryManager->persistentSourceProperties.wlock();
            lockedPersistentSourceProperties->erase(persistentSourceKey);
        }
    }
}

}// namespace NES
