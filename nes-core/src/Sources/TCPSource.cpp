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
#include <Util/UtilityFunctions.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Sources/TCPSource.hpp>
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <arpa/inet.h>
#include <unistd.h> // For read
#include <sys/socket.h> // For socket functions
#include <netinet/in.h> // For sockaddr_in

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
      tupleSize(schema->getSchemaSizeInBytes()), sourceConfig(std::move(tcpSourceType)) {
    NES_DEBUG("TCPSource::TCPSource " << this << ": Init TCPSource.");
}

TCPSource::~TCPSource() {
    NES_DEBUG("TCPSource::~TCPSource()");
    // Close the connections
    ::close(connection);
    ::close(sockfd);
    NES_DEBUG("TCPSource::~TCPSource  " << this << ": Destroy TCP Source");
}

std::string TCPSource::toString() const {
    std::stringstream ss;
    ss << "TCPSOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << sourceConfig->toString();
    return ss.str();
}

bool TCPSource::connected() {
    NES_TRACE("Trying to create socket.");
    sockfd = socket(sourceConfig->getSocketDomain()->getValue(), sourceConfig->getSocketType()->getValue(), 0);
    NES_TRACE("Socket created with " << sockfd);
    if (sockfd == -1) {
        NES_ERROR("Failed to create socket. errno: " << errno);
        connection = -1;
        return false;
    }
    NES_TRACE("Created socket");

    // Listen to port 9999 on any address
    struct sockaddr_in servaddr;
    servaddr.sin_family = sourceConfig->getSocketDomain()->getValue();
    servaddr.sin_addr.s_addr = inet_addr(sourceConfig->getSocketHost()->getValue().c_str());
    servaddr.sin_port =
        htons(sourceConfig->getSocketPort()->getValue());// htons is necessary to convert a number to network byte order

    connection = connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr));
    if (connection < 0) {
        NES_ERROR("Connection with server failed: " << errno);
        connection = -1;
        return false;
    }

    NES_TRACE("Connected to server.");
    return true;
}

std::optional<Runtime::TupleBuffer> TCPSource::receiveData() {
    NES_DEBUG("TCPSource  " << this << ": receiveData ");
    //todo: something is wrong with allocate buffer
    auto buffer = allocateBuffer();
    if (connected()) {
        if (!fillBuffer(buffer)) {
            NES_ERROR("TCPSource::receiveData: Failed to fill the TupleBuffer.");
            return std::nullopt;
        }
    } else {
        NES_ERROR("TCPSource::receiveData: Not connected!");
        return std::nullopt;
    }
    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer.getBuffer();
}

bool TCPSource::fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer) {

    // determine how many tuples fit into the buffer
    tuplesThisPass = tupleBuffer.getCapacity();
    NES_DEBUG("MQTTSource::fillBuffer: Fill buffer with #tuples=" << tuplesThisPass << " of size=" << tupleSize);

    uint64_t tupleCount = 0;

    while (tupleCount < tuplesThisPass){
        char* buffer = new char[tupleBuffer.getBuffer().getBufferSize()];
        //Todo: what buffer size to choose and how to convert the buffer to
        read(sock, buffer, tupleBuffer.getBuffer().getBufferSize());
        NES_TRACE("Client consume message: '" << buffer << "'");

        inputParser->writeInputTupleToTupleBuffer(buffer, tupleCount, tupleBuffer, schema);
    }

    tupleBuffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    return true;
}

SourceType TCPSource::getType() const { return TCP_SOURCE; }

}// namespace NES
