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

#include <HDFS/hdfs.h>
#include <NodeEngine/QueryManager.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/HdfsCSVSource.hpp>
#include <Util/Logger.hpp>
#include <boost/algorithm/string.hpp>
#include <sstream>
#include <string>
#include <Util/UtilityFunctions.hpp>

namespace NES {

HdfsCSVSource::HdfsCSVSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                             const std::string namenode, uint64_t port, const std::string hadoopUser, const std::string filePath, const std::string delimiter,
                             uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess, uint64_t frequency,
                             bool skipHeader, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId), namenode(namenode), port(port), hadoopUser(hadoopUser), filePath(filePath), delimiter(delimiter),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), currentPosInFile(0), skipHeader(skipHeader) {

    this->numBuffersToProcess = numBuffersToProcess;
    this->gatheringInterval = frequency;
    tupleSize = schema->getSchemaSizeInBytes();

    NES_DEBUG("HdfsCSVSource: Opening path " << filePath);
    char* path = const_cast<char*>(filePath.c_str());

    NES_DEBUG("HdfsCSVSource: Creating HdfsBuilder");
    this->builder = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(this->builder);
    hdfsBuilderSetNameNode(this->builder, namenode.c_str());
    hdfsBuilderSetNameNodePort(this->builder, port);
    hdfsBuilderSetUserName(this->builder, "hdoop");

    NES_DEBUG("HdfsCSVSource: Connecting to namenode " << namenode << ":" << port);
    this->fs = hdfsBuilderConnect(this->builder);
    if (!this->fs) {
        NES_ERROR("HdfsCSVSource: Could not connect to namenode");
    } else {
        NES_DEBUG("HdfsCSVSource: Connected to namenode " << namenode << ":" << port);
    }

    NES_DEBUG("HdfsCSVSource: Obtaining info from file: " << path);
    hdfsFileInfo *info = hdfsGetPathInfo(fs, path);
    NES_DEBUG("HdfsCSVSource: Information obtained:\n\tSize: " << info->mSize << "\n\tName: " << info->mName << "\n\tOwner: "
                                                               << info->mOwner);

    NES_DEBUG("HdfsCSVSource: opening in O_RDONLY mode the file: " << path);

    this->file = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs, path, O_RDONLY));
    if(!file) {
        NES_ERROR("HdfsCSVSource: Unable to open file " << path);
    } else {
        NES_DEBUG("HdfsCSVSource: Succesfully opened file " << path);
    }

    this->fileInfo = info;
    this->fileSize = this->fileInfo->mSize;

    if (fileSize == -1) {
        NES_ERROR("HdfsCSVSource::fillBuffer File " + filePath + " is corrupted");
    }

    if (numBuffersToProcess != 0) {
        loopOnFile = true;
    } else {
        loopOnFile = false;
    }

    NES_DEBUG("HdfsCSVSource: tupleSize=" << tupleSize << " freq=" << this->gatheringInterval
                                          << " numBuff=" << this->numBuffersToProcess << " numberOfTuplesToProducePerBuffer="
                                          << numberOfTuplesToProducePerBuffer << " loopOnFile=" << loopOnFile);

    fileEnded = false;
}

std::optional<NodeEngine::TupleBuffer> HdfsCSVSource::receiveData() {
    NES_DEBUG("HdfsCSVSource::receiveData called on " << operatorId);
    auto buf = this->bufferManager->getBufferBlocking();
    fillBuffer(buf);
    NES_DEBUG("HdfsCSVSource::receiveData filled buffer with tuples=" << buf.getNumberOfTuples());
    if (buf.getNumberOfTuples() == 0) {
        return std::nullopt;
    } else {
        NES_DEBUG(buf.getBufferAs<std::string>());
        return buf;
    }
}

const std::string HdfsCSVSource::toString() const {
    std::stringstream ss;
    ss << "Hdfs_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval
       << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

void HdfsCSVSource::fillBuffer(NodeEngine::TupleBuffer& buf) {
    NES_DEBUG("HdfsCSVSource::fillBuffer: start at pos=" << currentPosInFile << " fileSize=" << fileSize);
    if (fileEnded) {
        NES_WARNING("HdfsCSVSource::fillBuffer: but file has already ended");
        buf.setNumberOfTuples(0);
        return;
    }

    uint64_t generated_tuples_this_pass;
    //fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0) {
        generated_tuples_this_pass = buf.getBufferSize() / tupleSize;
    } else {
        generated_tuples_this_pass = numberOfTuplesToProducePerBuffer;
    }
    NES_DEBUG("HdfsCSVSource::fillBuffer: fill buffer with #tuples=" << generated_tuples_this_pass);

    char line[tupleSize + 1];
    uint64_t tupCnt = 0;
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (auto field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    uint64_t bytes_read = 0;

    if (skipHeader && currentPosInFile == 0) {
        NES_DEBUG("HdfsCSVSource: Skipping header");
        bytes_read = hdfsPread(fs, file, currentPosInFile, line, tupleSize);
        std::string line_str = line;
        size_t found = line_str.find("\n");
        line_str = line_str.substr(0, found + 1);
        currentPosInFile = bytes_read > 0 ? currentPosInFile + line_str.length() : -1;
    }

    while (tupCnt < generated_tuples_this_pass) {
        NES_DEBUG("HdfsCSVSource::fillBuffer: tupCnt=" << tupCnt << " generated_tuples_this_pass=" << generated_tuples_this_pass);
        if (currentPosInFile >= fileSize || currentPosInFile == -1) {
            NES_DEBUG("HdfsCSVSource::fillBuffer: reset tellg()=" << currentPosInFile << " file_size=" << fileSize);
            hdfsSeek(fs, file, 0);
            if (!loopOnFile) {
                NES_DEBUG("HdfsCSVSource::fillBuffer: break because file ended");
                fileEnded = true;
                break;
            }
            if (skipHeader) {
                NES_DEBUG("HdfsCSVSource: Skipping header");
                bytes_read = hdfsPread(fs, file, currentPosInFile, line, tupleSize);
                std::string line_str = line;
                size_t found = line_str.find("\n");
                line_str = line_str.substr(0, found + 1);
                currentPosInFile = bytes_read > 0 ? currentPosInFile + line_str.length() : -1;
            }
        }

        bytes_read = hdfsPread(fs, file, currentPosInFile, line, tupleSize);
        NES_DEBUG("HdfsCSVSource line=" << tupCnt << " val=" << line);
        std::vector<std::string> tokens;
        std::string line_str = line;
        size_t found = line_str.find("\n");
        line_str = line_str.substr(0, found + 1);
        NES_DEBUG("HdfsCSVSource line=" << tupCnt << " actual val=" << line_str);
        boost::algorithm::split(tokens, line_str, boost::is_any_of(this->delimiter));
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
        currentPosInFile = bytes_read > 0 ? currentPosInFile + line_str.length() : -1;
    }//end of while

    buf.setNumberOfTuples(tupCnt);
    NES_DEBUG("HdfsCSVSource::fillBuffer: reading finished read " << tupCnt << " tuples at posInFile=" << currentPosInFile);
    NES_DEBUG("HdfsCSVSource::fillBuffer: read produced buffer= " << UtilityFunctions::printTupleBufferAsCSV(buf, schema));

    //update statistics
    generatedTuples += tupCnt;
    generatedBuffers++;
}

SourceType HdfsCSVSource::getType() const { return HDFS_SOURCE; }

const std::string HdfsCSVSource::getFilePath() const { return filePath; }

const std::string HdfsCSVSource::getDelimiter() const { return delimiter; }

const uint64_t HdfsCSVSource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

bool HdfsCSVSource::getSkipHeader() const { return skipHeader; }

struct hdfsBuilder * HdfsCSVSource::getBuilder() { return builder; }
}// namespace NES
