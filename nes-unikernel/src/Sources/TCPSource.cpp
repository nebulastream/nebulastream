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
#include <Sources/TCPSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
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
extern NES::Runtime::BufferManagerPtr TheBufferManager;
TCPSource::TCPSource(TCPSourceTypePtr tcpSourceType,
                             OperatorId operatorId,
                             OriginId originId,
                             size_t numSourceLocalBuffers,
                             const std::string& physicalSourceName,
                             NES::Unikernel::UnikernelPipelineExecutionContext successor)
    : DataSource(operatorId, originId, numSourceLocalBuffers, physicalSourceName, std::move(successor)),
      tupleSize(schema->getSchemaSizeInBytes()), sourceConfig(std::move(tcpSourceType)), circularBuffer(2048) {
    NES_TRACE("TCPSource::TCPSource: Init TCPSource.");
}

template<typename Schema>
TCPSource::BufferType<Schema> TCPSource::allocateBuffer(){
    return BufferType<Schema> {TheBufferManager->getBufferBlocking()};
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
    NES_TRACE("TCPSource<Schema>::connected: Trying to create socket.");
    if (sockfd < 0) {
        sockfd = socket(sourceConfig->getSocketDomain()->getValue(), sourceConfig->getSocketType()->getValue(), 0);
        NES_TRACE("Socket created with  {}", sockfd);
    }
    if (sockfd < 0) {
        NES_ERROR("TCPSource<Schema>::connected: Failed to create socket. Error: {}", strerror(errno));
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
        NES_THROW_RUNTIME_ERROR("TCPSource<Schema>::connected: Connection with server failed. Error: " << strerror(errno));
    }
    NES_TRACE("TCPSource<Schema>::connected: Connected to server.");
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
    NES_TRACE("TCPSource<Schema>::close: trying to close connection.");
    DataSource::close();
    if (connection >= 0) {
        ::close(connection);
        ::close(sockfd);
        NES_TRACE("TCPSource<Schema>::close: connection closed.");
    }
}

SourceType TCPSource::getType() const { return SourceType::TCP_SOURCE; }

const TCPSourceTypePtr& TCPSource::getSourceConfig() const { return sourceConfig; }

}// namespace NES
