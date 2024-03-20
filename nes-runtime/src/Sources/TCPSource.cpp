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
#include <Sources/Parsers/CSVParser.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Sources/TCPSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <arpa/inet.h>
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
                     TCPSourceTypePtr tcpSourceType,
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
                 std::move(executableSuccessors)),
      tupleSize(schema->getSchemaSizeInBytes()), sourceConfig(std::move(tcpSourceType)), circularBuffer(2048) {

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
        NES_TRACE("TCPSOURCE:: Schema keys are:  {}", fieldName);
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

    NES_TRACE("TCPSource::TCPSource: Init TCPSource.");
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
    NES_TRACE("TCPSource::connected: Trying to create socket.");
    if (sockfd < 0) {
        sockfd = socket(sourceConfig->getSocketDomain()->getValue(), sourceConfig->getSocketType()->getValue(), 0);
        NES_TRACE("Socket created with  {}", sockfd);
    }
    if (sockfd < 0) {
        NES_ERROR("TCPSource::connected: Failed to create socket. Error: {}", strerror(errno));
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
        NES_TRACE("Try connecting to server: {}:{}",
                  sourceConfig->getSocketHost()->getValue(),
                  sourceConfig->getSocketPort()->getValue());
        connection = connect(sockfd, (struct sockaddr*) &servaddr, sizeof(servaddr));
    }
    if (connection < 0) {
        connection = -1;
        NES_THROW_RUNTIME_ERROR("TCPSource::connected: Connection with server failed. Error: " << strerror(errno));
    }
    NES_TRACE("TCPSource::connected: Connected to server.");
}

std::optional<Runtime::TupleBuffer> TCPSource::receiveData() {
    NES_DEBUG("TCPSource  {}: receiveData ", this->toString());
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
        delete[] messageBuffer;
        NES_ERROR("TCPSource::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw e;
    }
    return tupleBuffer.getBuffer();
}

bool TCPSource::fillBuffer(Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer) {

    // determine how many tuples fit into the buffer
    tuplesThisPass = tupleBuffer.getCapacity();
    NES_DEBUG("TCPSource::fillBuffer: Fill buffer with #tuples= {}  of size= {}", tuplesThisPass, tupleSize);

    //init tuple count for buffer
    uint64_t tupleCount = 0;
    //init timer for flush interval
    auto flushIntervalTimerStart = std::chrono::system_clock::now();
    //init flush interval value
    bool flushIntervalPassed = false;
    //need this to indicate that the whole tuple was successfully received from the circular buffer
    bool popped = true;
    //init tuple size
    uint64_t inputTupleSize = 0;
    //init size of received data from socket with 0
    int64_t bufferSizeReceived = 0;
    //receive data until tupleBuffer capacity reached or flushIntervalPassed
    while (tupleCount < tuplesThisPass && !flushIntervalPassed) {
        //if circular buffer is not full obtain data from socket
        if (!circularBuffer.full()) {
            //create new buffer with size equal to free space in circular buffer
            messageBuffer = new char[circularBuffer.capacity() - circularBuffer.size()];
            //fill created buffer with data from socket. Socket returns the number of bytes it actually sent.
            //might send more than one tuple at a time, hence we need to extract one tuple below in switch case.
            //user needs to specify how to find out tuple size when creating TCPSource
            bufferSizeReceived = read(sockfd, messageBuffer, circularBuffer.capacity() - circularBuffer.size());
            //if read method returned -1 an error occurred during read.
            if (bufferSizeReceived == -1) {
                delete[] messageBuffer;
                NES_ERROR("TCPSource::fillBuffer: an error occurred while reading from socket. Error: {}", strerror(errno));
                return false;
            }
            //if size of received data is not 0 (no data received), push received data to circular buffer
            else if (bufferSizeReceived != 0) {
                NES_DEBUG("TCPSOURCE::fillBuffer: bytes send: {}.", bufferSizeReceived);
                NES_DEBUG("TCPSOURCE::fillBuffer: print current buffer: {}.", messageBuffer);
                //push the received data into the circularBuffer
                circularBuffer.push(messageBuffer, bufferSizeReceived);
            }
            //delete allocated buffer
            delete[] messageBuffer;
        }

        if (!circularBuffer.empty()) {
            //switch case depends on the message receiving that was chosen when creating the source. Three choices are available:
            switch (sourceConfig->getDecideMessageSize()->getValue()) {
                // The user inputted a tuple separator that indicates the end of a tuple. We're going to search for that
                // tuple seperator and assume that all data until then belongs to the current tuple
                case Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR:
                    try {
                        // search the circularBuffer until Tuple seperator is found to obtain size of tuple
                        inputTupleSize = sizeUntilSearchToken(sourceConfig->getTupleSeparator()->getValue());
                        // allocate buffer with size of tuple
                        messageBuffer = new char[inputTupleSize];
                        NES_DEBUG("TCPSOURCE::fillBuffer: Pop Bytes from Circular Buffer to obtain Tuple of size: '{}'.",
                                  inputTupleSize);
                        NES_DEBUG("TCPSOURCE::fillBuffer: current circular buffer size: '{}'.", circularBuffer.size());
                        //copy and delete tuple from circularBuffer, delete tuple separator
                        popped = popGivenNumberOfValues(inputTupleSize, true);
                        break;
                    } catch (const std::exception& e) {
                        NES_ERROR("Failed to obtain tupleSize searching for separator token. Error: {}", e.what());
                        throw e;
                    }
                // The user inputted a fixed buffer size.
                case Configurations::TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE:
                    try {
                        //set tupleSize to user specified tuple size
                        inputTupleSize = sourceConfig->getSocketBufferSize()->getValue();
                        //allocate buffer with tupleSize
                        messageBuffer = new char[inputTupleSize];
                        NES_DEBUG("TCPSOURCE::fillBuffer: Pop Bytes from Circular Buffer to obtain Tuple of size: '{}'.",
                                  inputTupleSize);
                        NES_DEBUG("TCPSOURCE::fillBuffer: current circular buffer size: '{}'.", circularBuffer.size());
                        //copy and delete tuple from circularBuffer
                        popped = popGivenNumberOfValues(inputTupleSize, false);
                    } catch (const std::exception& e) {
                        NES_ERROR("Failed to obtain tupleSize with user inputted size. Error: {}", e.what());
                        throw e;
                    }
                    break;
                // Before each message, the server uses a fixed number of bytes (bytesUsedForSocketBufferSizeTransfer)
                // to indicate the size of the next tuple.
                case Configurations::TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET:
                    //when receiving buffer size from socket, we need to check that the buffer was actually popped during the last run, otherwise,
                    //we loose the transmitted size and obtain bytes from the tuple that weren't meant to transmit the size.
                    //This might happen if the size of the tuple was sent and popped, but we only received half of the tuple
                    //then we won't overwrite the tupleSize but try again to pop the next message.
                    if (popped) {
                        try {
                            NES_DEBUG("TCPSOURCE::fillBuffer: obtain socket buffer size");
                            //create buffer to save buffer size from socket in aka the number of bytes indicating the size of the next tuple
                            messageBuffer = new char[sourceConfig->getBytesUsedForSocketBufferSizeTransfer()->getValue() + 1];
                            //copy and delete the size of the next tuple from the circular buffer
                            popped = popGivenNumberOfValues(sourceConfig->getBytesUsedForSocketBufferSizeTransfer()->getValue(),
                                                            false);
                            NES_DEBUG("TCPSOURCE::fillBuffer: socket buffer size is: {}.", messageBuffer);
                            //if we successfully obtained the tuple size from the buffer convert bufferSizeFromSocket to an integer and delete the char*
                            if (popped) {
                                inputTupleSize = std::stoi(messageBuffer);
                                NES_DEBUG("TCPSOURCE::fillBuffer: socket buffer size is: {}.", inputTupleSize);
                            }
                            delete[] messageBuffer;
                        } catch (const std::exception& e) {
                            NES_ERROR("TCPSource::fillBuffer: Failed to retrieve the tupleSize from Message Buffer. Error: {}",
                                      e.what());
                            throw e;
                        }
                    }
                    //allocate the messageBuffer for one tuple with the new tupleSize
                    messageBuffer = new char[inputTupleSize + 1];
                    NES_TRACE("Pop Bytes from Circular Buffer to obtain Tuple of size: '{}'.", inputTupleSize);
                    NES_TRACE("current circular buffer size: '{}'.", circularBuffer.size());
                    //obtain the tuple from the circular buffer
                    popped = popGivenNumberOfValues(inputTupleSize, false);
                    break;
            }

            NES_TRACE("TCPSOURCE::fillBuffer: Successfully prepared tuples? '{}'", popped);
            //if we were able to obtain a complete tuple from the circular buffer, we are going to forward it ot the appropriate parser
            if (inputTupleSize != 0 && popped) {
                std::string buf(messageBuffer, inputTupleSize);
                NES_TRACE("TCPSOURCE::fillBuffer: Client consume message: '{}'.", buf);
                if (sourceConfig->getInputFormat()->getValue() == Configurations::InputFormat::JSON) {
                    NES_TRACE("TCPSOURCE::fillBuffer: Client consume message: '{}'.", buf);
                    inputParser->writeInputTupleToTupleBuffer(buf, tupleCount, tupleBuffer, schema, localBufferManager);
                } else {
                    inputParser->writeInputTupleToTupleBuffer(buf, tupleCount, tupleBuffer, schema, localBufferManager);
                }
                tupleCount++;
            }
            delete[] messageBuffer;
        }
        // If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
        // and writing data exceeds the user defined limit (bufferFlushIntervalMs).
        // If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
        if ((sourceConfig->getFlushIntervalMS()->getValue() > 0
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

uint64_t TCPSource::sizeUntilSearchToken(char token) {
    uint64_t places = 0;
    for (auto itr = circularBuffer.end() - 1; itr != circularBuffer.begin() - 1; --itr) {
        if (*itr == token) {
            return places;
        }
        ++places;
    }
    return places;
}

bool TCPSource::popGivenNumberOfValues(uint64_t numberOfValuesToPop, bool popTextDivider) {
    if (circularBuffer.size() >= numberOfValuesToPop) {
        for (uint64_t i = 0; i < numberOfValuesToPop; ++i) {
            char popped = circularBuffer.pop();
            messageBuffer[i] = popped;
        }
        messageBuffer[numberOfValuesToPop] = '\0';
        if (popTextDivider) {
            circularBuffer.pop();
        }
        return true;
    }
    return false;
}

void TCPSource::close() {
    NES_TRACE("TCPSource::close: trying to close connection.");
    DataSource::close();
    if (connection >= 0) {
        ::close(connection);
        ::close(sockfd);
        NES_TRACE("TCPSource::close: connection closed.");
    }
}

SourceType TCPSource::getType() const { return SourceType::TCP_SOURCE; }

const TCPSourceTypePtr& TCPSource::getSourceConfig() const { return sourceConfig; }

}// namespace NES
