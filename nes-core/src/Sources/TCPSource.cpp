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
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <Sources/TCPSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
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
                     TCPSourceTypePtr tcpSourceType,
                     OperatorId operatorId,
                     OriginId originId,
                     size_t numSourceLocalBuffers,
                     GatheringMode::Value gatheringMode,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(executableSuccessors)),
      tupleSize(schema->getSchemaSizeInBytes()), sourceConfig(std::move(tcpSourceType)),
      buffer(this->bufferManager->getBufferSize()) {
    NES_DEBUG("TCPSource::TCPSource " << this << ": Init TCPSource.");

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
        NES_TRACE("TCPSOURCE:: Schema keys are: " << fieldName);
        schemaKeys.push_back(fieldName.substr(fieldName.find('$') + 1, fieldName.size()));
    }

    switch (sourceConfig->getInputFormat()->getValue()) {
        case Configurations::InputFormat::JSON:
            inputParser = std::make_unique<JSONParser>(schema->getSize(), schemaKeys, physicalTypes);
            break;
        case Configurations::InputFormat::CSV:
            inputParser = std::make_unique<CSVParser>(schema->getSize(), physicalTypes, ",");
            break;
    }
}

TCPSource::~TCPSource() {
    NES_DEBUG("TCPSource::~TCPSource() with connection: " << connection);
    // Close the connections
    close();
    NES_DEBUG("TCPSource::~TCPSource  " << this << ": Destroy TCP Source");
}

std::string TCPSource::toString() const {
    std::stringstream ss;
    ss << "TCPSOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << sourceConfig->toString();
    return ss.str();
}

void TCPSource::open() {
    NES_TRACE("TCPSource::connected: Trying to create socket.");
    if (sockfd < 0) {
        sockfd = socket(sourceConfig->getSocketDomain()->getValue(), sourceConfig->getSocketType()->getValue(), 0);
        NES_TRACE("Socket created with " << sockfd);
    }
    if (sockfd == -1) {
        NES_ERROR("TCPSource::connected: Failed to create socket.");
        connection = -1;
        return;
    }
    NES_TRACE("Created socket");

    struct sockaddr_in servaddr;
    servaddr.sin_family = sourceConfig->getSocketDomain()->getValue();
    servaddr.sin_addr.s_addr = inet_addr(sourceConfig->getSocketHost()->getValue().c_str());
    servaddr.sin_port =
        htons(sourceConfig->getSocketPort()->getValue());// htons is necessary to convert a number to network byte order

    if (connection < 0) {
        NES_TRACE("Try connecting to server: " << sourceConfig->getSocketHost()->getValue() << ":"
                                               << sourceConfig->getSocketPort()->getValue());
        connection = connect(sockfd, (struct sockaddr*) &servaddr, sizeof(servaddr));
    }
    if (connection < 0) {
        NES_ERROR("TCPSource::connected: Connection with server failed. ");
        connection = -1;
        return;
    }

    NES_TRACE("TCPSource::connected: Connected to server.");
}

std::optional<Runtime::TupleBuffer> TCPSource::receiveData() {
    NES_DEBUG("TCPSource  " << this << ": receiveData ");
    open();
    if (connection != -1) {
        auto tupleBuffer = allocateBuffer();
        try {
            fillBuffer(tupleBuffer);
        } catch (std::exception e) {
            NES_ERROR("TCPSource::receiveData: Failed to fill the TupleBuffer.");
            return std::nullopt;
        }
        if (tupleBuffer.getNumberOfTuples() == 0) {
            return std::nullopt;
        }
        return tupleBuffer.getBuffer();
    } else {
        NES_ERROR("TCPSource::receiveData: Not connected!");
    }
    return std::nullopt;
}

bool TCPSource::fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer) {

    // determine how many tuples fit into the buffer
    tuplesThisPass = tupleBuffer.getCapacity();
    NES_DEBUG("TCPSource::fillBuffer: Fill buffer with #tuples=" << tuplesThisPass << " of size=" << tupleSize);

    uint64_t tupleCount = 0;
    auto flushIntervalTimerStart = std::chrono::system_clock::now();
    bool flushIntervalPassed = false;
    while (tupleCount < tuplesThisPass && !flushIntervalPassed) {

        uint32_t bufferSize = 0;
        if (sourceConfig->getSocketBufferSize()->getValue() == 0) {
            NES_TRACE("TCPSOURCE::fillBuffer: obtain socket buffer size");
            char* bufferSizeFromSocket = new char[4];
            uint8_t readSocket = read(sockfd, bufferSizeFromSocket, 4);
            NES_TRACE("TCPSOURCE::fillBuffer: socket buffer size is: " << bufferSizeFromSocket);
            if (readSocket != 0){
                bufferSize = std::stoi(bufferSizeFromSocket);
                NES_TRACE("TCPSOURCE::fillBuffer: socket buffer size is: " << bufferSize);
            }
            delete [] bufferSizeFromSocket;
        } else {
            bufferSize = sourceConfig->getSocketBufferSize()->getValue();
        }

        int16_t sendBytes;
        if (!buffer.full()) {
            void* buf = nullptr;
            sendBytes = read(sockfd, buf, buffer.capacity() - buffer.size());
            if (sendBytes != 0 && sendBytes != -1) {
                buffer.push(reinterpret_cast<char*>(&buf), sendBytes);
            }
        }

        uint64_t messageSize = buffer.sizeUntilSearchToken(0x03);
        char* messageBuffer = new char [messageSize];
        if (messageSize != 0 && buffer.popValuesUntil(messageBuffer, messageSize)) {
            NES_TRACE("TCPSOURCE::fillBuffer: Client consume message: '" << messageBuffer << "'");
            if (sourceConfig->getInputFormat()->getValue() == Configurations::InputFormat::JSON) {
                NES_TRACE("TCPSOURCE::fillBuffer: Client consume message: '" << messageBuffer << "'");
                inputParser->writeInputTupleToTupleBuffer(messageBuffer, tupleCount, tupleBuffer, schema);
            } else {
                inputParser->writeInputTupleToTupleBuffer(messageBuffer, tupleCount, tupleBuffer, schema);
            }
            tupleCount++;
        }
        delete[] messageBuffer;
        // If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
        // and writing data exceeds the user defined limit (bufferFlushIntervalMs).
        // If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
        if ((sourceConfig->getFlushIntervalMS()->getValue() > 0 && tupleCount > 0
             && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - flushIntervalTimerStart)
                     .count()
                 >= sourceConfig->getFlushIntervalMS()->getValue())) {
            NES_DEBUG("TCPSource::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
    }

    tupleBuffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    return true;
}

void TCPSource::close() {
    if (connection == 0) {
        ::close(connection);
        ::close(sockfd);
    }
}

SourceType TCPSource::getType() const { return TCP_SOURCE; }

}// namespace NES
