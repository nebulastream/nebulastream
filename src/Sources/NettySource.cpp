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
#include <NodeEngine/QueryManager.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/NettySource.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <arpa/inet.h>
#include <assert.h>
#include <boost/algorithm/string.hpp>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#define PORT 31000
#define BUFFER_SIZE 1024


using Clock = std::chrono::high_resolution_clock;

namespace NES {

NettySource::NettySource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string filePath,
                     const std::string delimiter, uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                     uint64_t frequency, bool endlessRepeat, bool skipHeader, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId), filePath(filePath), delimiter(delimiter),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), endlessRepeat(endlessRepeat), currentPosInFile(0),
      skipHeader(skipHeader) {
    this->numBuffersToProcess = numBuffersToProcess;
    this->gatheringInterval = frequency;
    tupleSize = schema->getSchemaSizeInBytes();
    NES_DEBUG("CSVSource: tupleSize=" << tupleSize << " freq=" << this->gatheringInterval
                                      << " numBuff=" << this->numBuffersToProcess << " numberOfTuplesToProducePerBuffer="
                                      << numberOfTuplesToProducePerBuffer << "endlessRepeat=" << endlessRepeat);

    char* path = realpath(filePath.c_str(), NULL);
    NES_DEBUG("CSVSource: Opening path " << path);
    input.open(path);

    NES_DEBUG("CSVSource::fillBuffer: read buffer");
    input.seekg(0, input.end);
    fileSize = input.tellg();
    if (fileSize == -1) {
        NES_ERROR("CSVSource::fillBuffer File " + filePath + " is corrupted");
    }
    fileEnded = false;
}

std::optional<TupleBuffer> NettySource::receiveData() {
    NES_DEBUG("CSVSource::receiveData called");
    auto buf = this->bufferManager->getBufferBlocking();
    //fillBuffer(buf);
    fillSocket(buf);
    NES_DEBUG("CSVSource::receiveData filled buffer with tuples=" << buf.getNumberOfTuples());
    return buf;
}

const std::string NettySource::toString() const {
    std::stringstream ss;
    ss << "CSV_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval
       << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

void NettySource::fillSocket(TupleBuffer& buf){

    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (auto field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }


    int sock = 0, valread;
    struct sockaddr_in serv_addr;
    char *hello = "0:persons\n";
    char buffer[1024] = {0};
    std::stringstream readStream;
    bool readData = true;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return ;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address/ Address not supported \n");
        return ;
    }

    if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        printf("\nConnection Failed \n");
        return ;
    }
    send(sock, hello, strlen(hello), 0);
    printf(0 + "persons");
    uint64_t tupCnt = 0;
    while(readData){

        bzero(buffer, BUFFER_SIZE);


        valread = read(sock, buffer, BUFFER_SIZE);

        if (valread <= 0)
        {
            break;
        }

        readStream << buffer;

        // Continue reading while end is not found.
        readData = readStream.str().find("end;") == std::string::npos;
        std::vector<std::string> parsed;
        boost::algorithm::split(parsed, buffer, boost::is_any_of("\n"));


        for(int i = 0 ; i < parsed.size(); i++ ){
            std::vector<std::string> tokens;
            boost::algorithm::split(tokens, parsed[i], boost::is_any_of(","));

            if(tokens.size() > 2 && !tokens[2].empty()  && !tokens[0].empty()  && !tokens[1].empty() && atoi( tokens[2].c_str() ) > 0){
                tokens.push_back(std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count()));
                uint64_t offset = 0;
                for (uint64_t j = 0; j < schema->getSize(); j++) {
                    auto field = physicalTypes[j];
                    uint64_t fieldSize = field->size();

                    if (field->isBasicType()) {
                        auto basicPhysicalField = std::dynamic_pointer_cast<BasicPhysicalType>(field);
                        /*
                     * TODO: this requires proper MIN / MAX size checks, numeric_limits<T>-like
                     * TODO: this requires underflow/overflow checks
                     * TODO: our types need their own sto/strto methods
                     */
                        if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_64) {

                            uint64_t val = std::stoull(tokens[j].c_str());
                            memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                        } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_64) {
                            int64_t val = std::stoll(tokens[j].c_str());
                            memcpy(buf.getBufferAs<char>() + offset + tupCnt * tupleSize, &val, fieldSize);
                        } else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_32) {
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

            }

        }
        //buf.setNumberOfTuples(tupCnt);
    }
    //  currentPosInFile = input.tellg();
    buf.setNumberOfTuples(tupCnt);
    //NES_DEBUG("CSVSource::fillBuffer: reading finished read " << tupCnt << " tuples at posInFile=" << currentPosInFile);
    NES_DEBUG("CSVSource::fillBuffer: read produced buffer= " << UtilityFunctions::printTupleBufferAsCSV(buf, schema));

    //update statistics
    generatedTuples += tupCnt;
    generatedBuffers++;

}






SourceType NettySource::getType() const { return CSV_SOURCE; }

const std::string NettySource::getFilePath() const { return filePath; }

const std::string NettySource::getDelimiter() const { return delimiter; }

const uint64_t NettySource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }
bool NettySource::isEndlessRepeat() const { return endlessRepeat; }
void NettySource::setEndlessRepeat(bool endlessRepeat) { this->endlessRepeat = endlessRepeat; }

bool NettySource::getSkipHeader() const { return skipHeader; }
}// namespace NES
