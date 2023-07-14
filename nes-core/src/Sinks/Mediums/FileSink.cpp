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

#include <Runtime/NodeEngine.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <filesystem>
#include <iostream>
#include <regex>
#include <string>
#include <utility>

namespace NES {

SinkMediumTypes FileSink::getSinkMediumType() { return FILE_SINK; }

FileSink::FileSink(SinkFormatPtr format,
                   Runtime::NodeEnginePtr nodeEngine,
                   uint32_t numOfProducers,
                   const std::string& filePath,
                   bool append,
                   QueryId queryId,
                   QuerySubPlanId querySubPlanId,
                   FaultToleranceType::Value faultToleranceType,
                   uint64_t numberOfOrigins)
    : SinkMedium(std::move(format),
                 std::move(nodeEngine),
                 numOfProducers,
                 queryId,
                 querySubPlanId,
                 faultToleranceType,
                 numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)) {
    this->filePath = filePath;
    this->append = append;
    if (!append) {
        if (std::filesystem::exists(filePath.c_str())) {
            bool success = std::filesystem::remove(filePath.c_str());
            NES_ASSERT2_FMT(success, "cannot remove file " << filePath.c_str());
        }
    }
    NES_DEBUG("FileSink: open file=" << filePath);
    if (!outputFile.is_open()) {
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
    }
    NES_ASSERT(outputFile.is_open(), "file is not open");
    NES_ASSERT(outputFile.good(), "file not good");
}

FileSink::~FileSink() {
    NES_DEBUG("~FileSink: close file=" << filePath);
    outputFile.close();
}

std::string FileSink::toString() const {
    std::stringstream ss;
    ss << "FileSink(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void FileSink::setup() {}

void FileSink::shutdown() {}

bool FileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    std::unique_lock lock(writeMutex);
    NES_TRACE("FileSink: getSchema medium " << toString() << " format " << sinkFormat->toString() << " and mode "
                                            << this->getAppendAsString());

    if (!inputBuffer) {
        NES_ERROR("FileSink::writeData input buffer invalid");
        return false;
    }
    if (!schemaWritten) {
        NES_TRACE("FileSink::getData: write schema");
        auto schemaBuffer = sinkFormat->getSchema();
        if (schemaBuffer) {
            std::ofstream outputFile;
            if (sinkFormat->getSinkFormat() == NES_FORMAT) {
                uint64_t idx = filePath.rfind('.');
                std::string shrinkedPath = filePath.substr(0, idx + 1);
                std::string schemaFile = shrinkedPath + "schema";
                NES_TRACE("FileSink::writeData: schema is =" << sinkFormat->getSchemaPtr()->toString()
                                                             << " to file=" << schemaFile);
                outputFile.open(schemaFile, std::ofstream::binary | std::ofstream::trunc);
            } else {
                outputFile.open(filePath, std::ofstream::binary | std::ofstream::trunc);
            }

            outputFile.write((char*) schemaBuffer->getBuffer(), schemaBuffer->getNumberOfTuples());
            outputFile.close();

            schemaWritten = true;
            NES_TRACE("FileSink::writeData: schema written");
        } else {
            NES_TRACE("FileSink::writeData: no schema written");
        }
    } else {
        NES_TRACE("FileSink::getData: schema already written");
    }

    NES_TRACE("FileSink::getData: write data to file=" << filePath);
    auto dataBuffers = sinkFormat->getData(inputBuffer);

    for (auto& buffer : dataBuffers) {
        NES_TRACE("FileSink::getData: write buffer of size " << buffer.getNumberOfTuples());
        std::string str;
        str.assign((char*) buffer.getBuffer(), buffer.getNumberOfTuples());
        auto timestamp = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        std::string repReg = std::to_string(timestamp);
        repReg = "," + repReg + "\n";
        str = std::regex_replace(str, std::regex(R"(\n)"), repReg);

        NES_TRACE("FileSink::getData: received following content: \n" << str);

        if (sinkFormat->getSinkFormat() == NES_FORMAT) {
            //outputFile.write((char*) buffer.getBuffer(),
            //                buffer.getNumberOfTuples() * sinkFormat->getSchemaPtr()->getSchemaSizeInBytes());
        } else {
            //outputFile.write((char*) buffer.getBuffer(), buffer.getNumberOfTuples());
        }
        outputFile.write(str.c_str(), str.length());
    }
    outputFile.flush();
    updateWatermarkCallback(inputBuffer);
    return true;
}

std::string FileSink::getFilePath() const { return filePath; }

}// namespace NES
