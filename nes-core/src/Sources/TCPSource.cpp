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

#include <Util/UtilityFunctions.hpp>
#include <Sources/TCPSource.hpp>
#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>

namespace NES {

TCPSource::TCPSource(SchemaPtr schema,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     const TCPSourceTypePtr& tcpSourceType,
                     OperatorId operatorId,
                     OriginId originId,
                     size_t numSourceLocalBuffers,
                     GatheringMode::Value gatheringMode,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
    : DataSource(schema,
                 bufferManager,
                 queryManager,
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(executableSuccessors)),tupleSize(schema->getSchemaSizeInBytes()), sourceConfig(std::move(tcpSourceType)) {
    NES_DEBUG("TCPSource::TCPSource " << this << ": Init TCPSource.");
}

TCPSource::~TCPSource() {
    NES_DEBUG("TCPSource::~TCPSource()");

    NES_DEBUG("TCPSource::~TCPSource  " << this << ": Destroy TCP Source");
}

std::string TCPSource::toString() const {
    std::stringstream ss;
    ss << "TCPSOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "URL=" << sourceConfig->getUrl() << ".";
    return ss.str();
}

bool TCPSource::connected() {
    struct sockaddr_in serv_addr;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
        NES_ERROR("TCPSource::connect: could not create socket.");
        connection = -1;
        return false;
    }
    serv_addr.sin_family = AF_INET;
    std::vector<std::string> connectionConfigs = Util::splitWithStringDelimiter<std::string>(sourceConfig->getUrl()->getValue(), ":");
    serv_addr.sin_port = htons(std::stoi(connectionConfigs.at(2)));

    if (inet_pton(AF_INET, connectionConfigs.at(1).c_str(), &serv_addr.sin_addr) <= 0){
        NES_ERROR("TCPSource::connect: address: " << connectionConfigs.at(1) << " not supported");
        connection = -1;
        return false;
    }

    if ((connection = connect(sock, (struct sockaddr*)&serv_addr, sizeof (serv_addr))) < 0){
        NES_ERROR("TPCSource::connect: connection failed");
        return false;
    }
    return true;
}

std::optional<Runtime::TupleBuffer> TCPSource::receiveData() {
    NES_DEBUG("TCPSource  " << this << ": receiveData ");
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
    //Todo: what buffer size to choose and how to convert the buffer to
    //read(sock, tupleBuffer.getBuffer(), tupleBuffer.getBuffer().getBufferSize());



    return true;
}

}// namespace NES
