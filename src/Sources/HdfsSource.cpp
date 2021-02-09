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

#include <NodeEngine/QueryManager.hpp>
#include <Sources/HdfsSource.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger.hpp>
#include <boost/algorithm/string.hpp>
#include <HDFS/hdfs.h>
#include <sstream>
#include <string>
#include <vector>

namespace NES {

HdfsSource::HdfsSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                       const std::string namenode, uint64_t port, const std::string hadoopUser, const std::string filePath, const std::string delimiter,
                       uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess, uint64_t frequency,
                       bool skipHeader, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId), namenode(namenode), port(port), hadoopUser(hadoopUser), filePath(filePath), delimiter(delimiter),
      numberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer), currentPosInFile(0), skipHeader(skipHeader) {

    this->numBuffersToProcess = numBuffersToProcess;
    this->gatheringInterval = frequency;
    tupleSize = schema->getSchemaSizeInBytes();

    NES_DEBUG("HdfsSource: Opening path " << filePath);
    char* path = const_cast<char*>(filePath.c_str());

    NES_DEBUG("HdfsSource: Creating HdfsBuilder");
    this->builder = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(this->builder);
    hdfsBuilderSetNameNode(this->builder, namenode.c_str());
    hdfsBuilderSetNameNodePort(this->builder, port);
    hdfsBuilderSetUserName(this->builder, "hdoop");

    NES_DEBUG("HdfsSource: Connecting to namenode " << namenode << ":" << port);
    this->fs = hdfsBuilderConnect(this->builder);
    if (!this->fs) {
        NES_ERROR("HdfsSource: Could not connect to namenode");
    } else {
        NES_DEBUG("HdfsSource: Connected to namenode " << namenode << ":" << port);
    }

    NES_DEBUG("HdfsSource: Obtaining info from file: " << path);
    hdfsFileInfo *info = hdfsGetPathInfo(fs, path);
    NES_DEBUG("HdfsSource: Information obtained:\n\tSize: " << info->mSize << "\n\tName: " << info->mName << "\n\tOwner: "
                                                            << info->mOwner);

    NES_DEBUG("HdfsSource: opening in O_RDONLY mode the file: " << path);

    this->file = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs, path, O_RDONLY));
    if(!file) {
        NES_ERROR("HdfsSource: Unable to open file " << path);
    } else {
        NES_DEBUG("HdfsSource: Succesfully opened file " << path);
    }

    this->fileInfo = info;
    this->fileSize = this->fileInfo->mSize;

    if (fileSize == -1) {
        NES_ERROR("HdfsSource::fillBuffer File " + filePath + " is corrupted");
    }

    if (numBuffersToProcess != 0) {
        loopOnFile = true;
    } else {
        loopOnFile = false;
    }

    NES_DEBUG("HdfsSource: tupleSize=" << tupleSize << " freq=" << this->gatheringInterval
                                      << " numBuff=" << this->numBuffersToProcess << " numberOfTuplesToProducePerBuffer="
                                      << numberOfTuplesToProducePerBuffer << " loopOnFile=" << loopOnFile);

    fileEnded = false;
}

std::optional<NodeEngine::TupleBuffer> HdfsSource::receiveData() {
    NES_DEBUG("HdfsSource::receiveData called on " << operatorId);
    auto buf = this->bufferManager->getBufferBlocking();
    fillBuffer(buf);
    NES_DEBUG("HdfsSource::receiveData filled buffer with tuples=" << buf.getNumberOfTuples());
    if (buf.getNumberOfTuples() == 0) {
        return std::nullopt;
    } else {
        NES_DEBUG(buf.getBufferAs<std::string>());
        return buf;
    }
}

const std::string HdfsSource::toString() const {
    std::stringstream ss;
    ss << "Hdfs_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval
       << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

void HdfsSource::fillBuffer(NodeEngine::TupleBuffer& buf) {

    if (input.tellg() == fileSize) {
        input.seekg(0, input.beg);
    }
    uint64_t uint64_to_read = buf.getBufferSize() < (uint64_t) fileSize ? buf.getBufferSize() : fileSize;
    hdfsRead(fs, file, buf.getBufferAs<char>(), uint64_to_read);
    uint64_t generated_tuples_this_pass = uint64_to_read / tupleSize;
    NES_DEBUG("HdfsSource::fillBuffer: generated tuples this pass: " << generated_tuples_this_pass);
    buf.setNumberOfTuples(generated_tuples_this_pass);

    generatedTuples += generated_tuples_this_pass;
    generatedBuffers++;
}

SourceType HdfsSource::getType() const { return HDFS_SOURCE; }

const std::string HdfsSource::getFilePath() const { return filePath; }

const std::string HdfsSource::getDelimiter() const { return delimiter; }

const uint64_t HdfsSource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

bool HdfsSource::getSkipHeader() const { return skipHeader; }

struct hdfsBuilder *HdfsSource::getBuilder() { return builder; }
}// namespace NES
