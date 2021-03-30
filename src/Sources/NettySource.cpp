/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <NodeEngine/FixedSizeBufferPool.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/NettySource.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <arpa/inet.h>
#include <boost/algorithm/string.hpp>
#include <chrono>
#include <iostream>
#include <queue>
#include <regex>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#define PORT 5000
#define BUFFER_SIZE 4096

using Clock = std::chrono::high_resolution_clock;
//std::stringstream readStream;
std::string str;
int size = 0;
int sock = 0;

namespace NES {

NettySource::NettySource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                         const std::string filePath, const std::string delimiter, uint64_t numberOfTuplesToProducePerBuffer,
                         uint64_t numBuffersToProcess, uint64_t frequency, bool skipHeader, OperatorId operatorId,  std::string address, size_t numSourceLocalBuffers)
    : DataSource(schema, bufferManager, queryManager, operatorId, numSourceLocalBuffers), filePath(filePath), delimiter(delimiter),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), currentPosInFile(0),
      skipHeader(skipHeader),address(address) {
    this->numBuffersToProcess = numBuffersToProcess;
    this->gatheringInterval = std::chrono::milliseconds(frequency);
    tupleSize = schema->getSchemaSizeInBytes();
    this->address = address;

    /*  NES_DEBUG("CSVSource: tupleSize=" << tupleSize << " freq=" << this->gatheringInterval
                                      << " numBuff=" << this->numBuffersToProcess << " numberOfTuplesToProducePerBuffer="
                                      << numberOfTuplesToProducePerBuffer << "endlessRepeat=" << endlessRepeat);
*/
    char* path = realpath(filePath.c_str(), NULL);
    NES_DEBUG("NettySource: Opening path " << path);
    input.open(path);

    NES_DEBUG("NettySource::fillBuffer: read buffer");
    input.seekg(0, input.end);
    fileSize = input.tellg();
    if (fileSize == -1) {
        NES_ERROR("NettySource::fillBuffer File " + filePath + " is corrupted");
    }
    if (numBuffersToProcess != 0) {
        loopOnFile = true;
    } else {
        loopOnFile = false;
    }
    NES_DEBUG("NettySource: tupleSize=" << tupleSize << " freq=" << this->gatheringInterval.count() << "ms"
                                      << " numBuff=" << this->numBuffersToProcess << " numberOfTuplesToProducePerBuffer="
                                      << numberOfTuplesToProducePerBuffer << "loopOnFile=" << loopOnFile);

    fileEnded = false;

//connection to netty socket
    struct sockaddr_in serv_addr;
    sock = 0;
    //int valread;

    //bool readData = true;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        NES_ERROR("\n Socket creation error \n");
        return;
    }


    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    //172.16.0.254
    // Convert IPv4 and IPv6 addresses from text to binary form
    if (inet_pton(AF_INET,address.c_str(), &serv_addr.sin_addr) <= 0) {
        NES_ERROR(" Invalid address/ Address not supported ");
        NES_ERROR(address.c_str());
        return;
    }

    if (connect(sock, (struct sockaddr*) &serv_addr, sizeof(serv_addr)) < 0) {
        NES_ERROR("Connection Failed");
        return;
    }
    char* hello = "0:bids\n";

    send(sock, hello, strlen(hello), 0);

}


std::optional<NodeEngine::TupleBuffer> NettySource::receiveData() {
    NES_DEBUG("NettySource::receiveData called");

    //auto buf = this->bufferManager->getBufferBlocking();
    NES_DEBUG("NettySource::new Buffer");
    auto buf = bufferManager->getBufferBlocking();
    fillSocket(buf);
    NES_DEBUG("NettySource::receiveData filled buffer with tuples=" << buf.getNumberOfTuples());

     return buf;
}

const std::string NettySource::toString() const {
    std::stringstream ss;
    ss << "Netty_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count() << "ms"
       << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

void NettySource::fillSocket(NodeEngine::TupleBuffer& buf) {

         uint64_t tupCnt = 0;

        NES_DEBUG("NettySource::inside Fill Socket");
        std::vector<PhysicalTypePtr> physicalTypes;
        DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
        for (auto field : schema->fields) {
            //NES_DEBUG("NettySource::schema size" << schema->getSize() << " schema bytes" << schema->getSchemaSizeInBytes());
            auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
            physicalTypes.push_back(physicalField);
        }

        std::regex validity("^[a-zA-Z0-9,]+$");

        //readStream << buffer;

        // Continue reading while end is not found.
        // readData = readStream.str().find("end;") == std::string::npos;
        std::deque<std::string> parsed;
        //bool clearedReadStream = false;
         while(tupCnt * schema->getSchemaSizeInBytes() + schema->getSchemaSizeInBytes() < buf.getBufferSize()) {

            if (parsed.empty()) {
                char buffer[4096] = {0};
                bzero(buffer, BUFFER_SIZE);
                int valread = read(sock, buffer, BUFFER_SIZE);
                NES_DEBUG("NettySource:: Read Buffer");
                if (valread <= 0) {
                    NES_TRACE("NettySource::fillBuffer: read produced buffer= "
                              << UtilityFunctions::printTupleBufferAsCSV(buf, schema));
                    buf.setNumberOfTuples(0);
                    return;
                }
                boost::algorithm::split(parsed, buffer, boost::is_any_of("\n"));
            }
            else {
                std::string strd = parsed.front();
                parsed.pop_front();
                //NES_DEBUG("NettySource:: Inside For loop");
                std::vector<std::string> tokens;
                boost::algorithm::split(tokens, strd, boost::is_any_of(","));
                if (tokens.size() != 5) {
                    str = str + strd;
                    boost::algorithm::split(tokens, str, boost::is_any_of(","));
                } else
                    str = strd;

                /* if ((tupCnt * schema->getSchemaSizeInBytes()) + schema->getSchemaSizeInBytes() > buf.getBufferSize()) {
                    buf.setNumberOfTuples(tupCnt);
                    NES_DEBUG("NettySource::fillBuffer: read produced buffer= " << buf.getNumberOfTuples());
                    NES_TRACE("NettySource::fillBuffer: read produced buffer= " << UtilityFunctions::printTupleBufferAsCSV(buf, schema));
                    queryManager->addWork(operatorId, buf);
                    buf = bufferManager->getBufferBlocking();
                    tupCnt = 0;
                }*/
                if (std::regex_match(strd.c_str(), validity)) {
                    if (tokens.size() == 5 && !tokens[2].empty() && !tokens[0].empty() && !tokens[1].empty()
                        && !tokens[4].empty()) {
                        tokens.push_back(std::to_string(
                            std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count()));
                        uint64_t offset = 0;
                        for (uint64_t j = 0; j < schema->getSize(); j++) {
                            auto field = physicalTypes[j];
                            uint64_t fieldSize = field->size();

                            if (field->isBasicType()) {
                                auto basicPhysicalField = std::dynamic_pointer_cast<BasicPhysicalType>(field);
                                /*     * TODO: this requires proper MIN / MAX size checks, numeric_limits<T>-like
                         * TODO: this requires underflow/overflow checks
                         * TODO: our types need their own sto/strto methods
    */
                                /*     NES_ASSERT2_FMT(fieldSize + offset + tupCnt * tupleSize < buf.getBufferSize(),
                                            "Overflow detected: buffer size = " << buf.getBufferSize()
                                                                                << " position = " << (offset + tupCnt * tupleSize)
                                                                                << " field size " << fieldSize);*/
                                if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_64) {

                                    uint64_t val = std::stoull(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_64) {
                                    int64_t val = std::stoll(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_32) {
                                    //NES_DEBUG("NettySource:: " << parsed[i].c_str());
                                    uint32_t val = std::stoul(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_32) {
                                    int32_t val = std::stol(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16) {
                                    uint16_t val = std::stol(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_16) {
                                    int16_t val = std::stol(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16) {
                                    uint8_t val = std::stoi(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_8) {
                                    int8_t val = std::stoi(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_8) {
                                    int8_t val = std::stoi(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::DOUBLE) {
                                    double val = std::stod(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::FLOAT) {
                                    float val = std::stof(tokens[j].c_str());
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::BOOLEAN) {
                                    bool val = (strcasecmp(tokens[j].c_str(), "true") == 0 || atoi(tokens[j].c_str()) != 0);
                                    memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                                }
                            } else {
                                memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, tokens[j].c_str(), fieldSize);
                            }

                            offset += fieldSize;
                        }
                        tupCnt++;
                        str.clear();
                    } else {
                        NES_WARNING("Incomplete token received " << strd);
                        //NES_INFO(" buffer size " << buf.getBufferSize());
                        tupCnt++;
                    }
                } else {
                    NES_WARNING("Wrong token" << strd);
                    tupCnt++;
                    str.clear();
                }

                buf.setNumberOfTuples(tupCnt);
            }
        }


   // buf.setNumberOfTuples(tupCnt);
    NES_DEBUG("NettySource:: Buffer Full: ");
    NES_TRACE("NettySource:: fillBuffer: read produced buffer= " << UtilityFunctions::printTupleBufferAsCSV(buf, schema));


    //update statistics
    generatedTuples += tupCnt;
    generatedBuffers++;
    NES_DEBUG("NettySource:: generatedTuples: " << generatedTuples << ": Buffercount " << generatedBuffers);

}

SourceType NettySource::getType() const { return CSV_SOURCE; }

const std::string NettySource::getFilePath() const { return filePath; }

const std::string NettySource::getDelimiter() const { return delimiter; }
const std::string NettySource::getAddress()  const { return address; }

const uint64_t NettySource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }
/*
bool NettySource::isEndlessRepeat() const { return endlessRepeat; }
void NettySource::setEndlessRepeat(bool endlessRepeat) { this->endlessRepeat = endlessRepeat; }
*/

bool NettySource::getSkipHeader() const { return skipHeader; }
}// namespace NES
