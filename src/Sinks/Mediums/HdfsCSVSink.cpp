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
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Mediums/HdfsCSVSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <iostream>
#include <string>
#include <utility>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <fstream>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES {

std::string HdfsCSVSink::toString() { return "HDFS_SINK"; }

SinkMediumTypes HdfsCSVSink::getSinkMediumType() { return FILE_SINK; }

HdfsCSVSink::HdfsCSVSink(SinkFormatPtr format, char *filePath, bool append, QuerySubPlanId parentPlanId)
    : SinkMedium(std::move(format), parentPlanId) {
    this->filePath = filePath;
    this->append = append;

    hdfsBuilder *bld = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderSetNameNode(bld, "192.168.1.104");
    hdfsBuilderSetNameNodePort(bld, 9000);
    hdfsBuilderSetUserName(bld, "hdoop");
    fs = hdfsBuilderConnect(bld);
}

HdfsCSVSink::~HdfsCSVSink() {
    NES_DEBUG("HdfsCSVSinkink: close file=" << filePath);
    if (hdfsFileIsOpenForWrite(outputFile)) {
        hdfsCloseFile(fs, outputFile);
    }
}

const std::string HdfsCSVSink::toString() const {
    std::stringstream ss;
    ss << "HdfsCSVSink(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    return ss.str();
}

void HdfsCSVSink::setup() {}

void HdfsCSVSink::shutdown() {}

bool HdfsCSVSink::writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContextRef) {
    std::unique_lock lock(writeMutex);

    NES_DEBUG("HdfsCSVSink: getSchema medium " << toString() << " format " << sinkFormat->toString() << " and mode "
                                            << this->getAppendAsString());

    if (!append) {
        outputFile = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs, filePath, O_WRONLY | O_CREAT));
    } else {
        outputFile = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs, filePath, O_WRONLY | O_APPEND));
    }

    if (!inputBuffer.isValid()) {
        NES_ERROR("HdfsCSVSink::writeData input buffer invalid");
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
        NES_DEBUG("HdfsCSVSink::getData: schema already written");
    }
    auto numberOfTuples = inputBuffer.getNumberOfTuples();
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto buffer = inputBuffer.getBufferAs<char>();

    NES_DEBUG("HdfsCSVSink::getData: write data to file=" << filePath);
    for(uint64_t i = 0; i < numberOfTuples; i++) {
        std::stringstream ss;
        uint64_t offset = 0;
        // the schema is previously created and given as argument to the createKafkaSink =  SinkFormat
        SchemaPtr schema = sinkFormat->getSchemaPtr();
        for (uint64_t j = 0; j < schema->getSize(); j++) {
            auto field = schema->get(j);
            auto ptr = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(ptr);
            auto fieldSize = physicalType->size();
            auto str = physicalType->convertRawToString(buffer + offset + i *  schema->getSchemaSizeInBytes());
            ss << str.c_str();
            if (j < schema->getSize() - 1) {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << "\n";
        char *line = const_cast<char*>(ss.str().c_str());
        NES_INFO("Writing line: " << ss.str().c_str());
        hdfsWrite(fs, outputFile, line, ss.str().length() + 1);

    }
    hdfsFlush(fs, outputFile);
    hdfsCloseFile(fs, outputFile);

    return true;
}

const char * HdfsCSVSink::getFilePath() const { return filePath; }

}// namespace NES
