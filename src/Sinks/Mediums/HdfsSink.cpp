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

#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Mediums/HdfsSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <iostream>
#include <string>
#include <utility>
#include <HDFS/hdfs.h>

namespace NES {

std::string HdfsSink::toString() { return "HDFS_SINK"; }

SinkMediumTypes HdfsSink::getSinkMediumType() { return FILE_SINK; }

HdfsSink::HdfsSink(SinkFormatPtr format, char *filePath, bool append, QuerySubPlanId parentPlanId)
    : SinkMedium(std::move(format), parentPlanId) {
    this->filePath = filePath;
    this->append = append;

    hdfsBuilder *bld = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderSetNameNode(bld, "192.168.1.104");
    hdfsBuilderSetNameNodePort(bld, 9000);
    hdfsBuilderSetUserName(bld, "hdoop");
    fs = hdfsBuilderConnect(bld);

//    NES_DEBUG("HdfsSink: open file=" << filePath);
//    if (!append) {
//        outputFile = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs, filePath, O_WRONLY | O_CREAT));
//    } else {
//        outputFile = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs, filePath, O_WRONLY | O_APPEND));
//    }
//    NES_ASSERT(outputFile, "Failed to open file for writing");
}

HdfsSink::~HdfsSink() {
    NES_DEBUG("~HdfsSink: close file=" << filePath);
    if (hdfsFileIsOpenForWrite(outputFile)) {
        hdfsCloseFile(fs, outputFile);
    }
}

const std::string HdfsSink::toString() const {
    std::stringstream ss;
    ss << "HdfsSink(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    return ss.str();
}

void HdfsSink::setup() {}

void HdfsSink::shutdown() {}

bool HdfsSink::writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContextRef) {
    std::unique_lock lock(writeMutex);

    NES_DEBUG("HdfsSink: getSchema medium " << toString() << " format " << sinkFormat->toString() << " and mode "
                                            << this->getAppendAsString());

    if (!append) {
        outputFile = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs, filePath, O_WRONLY | O_CREAT));
    } else {
        outputFile = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs, filePath, O_WRONLY | O_APPEND));
    }

    if (!inputBuffer.isValid()) {
        NES_ERROR("HdfsSink::writeData input buffer invalid");
        return false;
    }
    if (!schemaWritten) {
        NES_DEBUG("FileSink::getData: write schema");
        auto schemaBuffer = sinkFormat->getSchema();
        if (schemaBuffer) {
            std::ofstream outputFile;
            if (sinkFormat->getSinkFormat() == NES_FORMAT) {
                std::string path = filePath;
                uint64_t idx = path.rfind(".");
                std::string shrinkedPath = path.substr(0, idx + 1);
                std::string schemaFile = "../tests/test_data/sink.schema";
                NES_DEBUG("FileSink::writeData: schema is =" << sinkFormat->getSchemaPtr()->toString()
                                                             << " to file=" << schemaFile);
                outputFile.open(schemaFile, std::ofstream::binary | std::ofstream::trunc);
            } else {
                outputFile.open(filePath, std::ofstream::binary | std::ofstream::trunc);
            }

            outputFile.write((char*) schemaBuffer->getBuffer(), schemaBuffer->getNumberOfTuples());
            outputFile.close();

            schemaWritten = true;
            NES_DEBUG("FileSink::writeData: schema written");
        } else {
            NES_DEBUG("FileSink::writeData: no schema written");
        }
    } else {
        NES_DEBUG("HdfsSink::getData: schema already written");
    }

    NES_DEBUG("HdfsSink::getData: write data to file=" << filePath);
    auto dataBuffers = sinkFormat->getData(inputBuffer);

    for (auto buffer : dataBuffers) {
        NES_DEBUG("HdfsSink::writeData: write buffer of size " << buffer.getNumberOfTuples());
        hdfsWrite(fs, outputFile, (char*) buffer.getBuffer(), buffer.getBufferSize());
        NES_DEBUG("HdfsSink::writeData: data written: " << (char*) buffer.getBuffer());
    }
    hdfsFlush(fs, outputFile);
    hdfsCloseFile(fs, outputFile);

    return true;
}

const char *HdfsSink::getFilePath() const { return filePath; }

}// namespace NES
