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
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <filesystem>
#include <iostream>
#include <regex>
#include <string>
#include <utility>

namespace NES {
constexpr bool WRITE_ALL_ON_SHUTDOWN = true;

SinkMediumTypes FileSink::getSinkMediumType() { return SinkMediumTypes::FILE_SINK; }

FileSink::FileSink(SinkFormatPtr format,
                   Runtime::NodeEnginePtr nodeEngine,
                   uint32_t numOfProducers,
                   const std::string& filePath,
                   bool append,
                   QueryId queryId,
                   DecomposedQueryPlanId querySubPlanId,
                   uint64_t numberOfOrigins)
    : SinkMedium(std::move(format), std::move(nodeEngine), numOfProducers, queryId, querySubPlanId, numberOfOrigins) {
    this->filePath = filePath;
    this->append = append;
    if (!append) {
        if (std::filesystem::exists(filePath.c_str())) {
            bool success = std::filesystem::remove(filePath.c_str());
            NES_ASSERT2_FMT(success, "cannot remove file " << filePath.c_str());
        }
    }
    NES_DEBUG("FileSink: open file= {}", filePath);

    // open the file stream
    if (!outputFile.is_open()) {
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
    }
    NES_ASSERT(outputFile.is_open(), "file is not open");
    NES_ASSERT(outputFile.good(), "file not good");
}

FileSink::~FileSink() {
    NES_DEBUG("~FileSink: close file={}", filePath);
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

void FileSink::shutdown() {
    if (WRITE_ALL_ON_SHUTDOWN) {
//        sinkFormat->setAddTimestamp(true);
//        std::vector<std::string> timestampedOutputStrings;
//        timestampedOutputStrings.reserve(receivedBuffers.size());
//        for (uint64_t i = 0; i < receivedBuffers.size(); ++i) {
//            //auto inputBuffer = receivedBuffers[i];
//            auto bufferContent = receivedBuffers[i];
//            auto timestamp = arrivalTimestamps[i];
//
//            NES_DEBUG("FileSink: getSchema medium {} format {} mode {}",
//                      toString(),
//                      sinkFormat->toString(),
//                      this->getAppendAsString());
//
////            if (!inputBuffer) {
////                NES_ERROR("FileSink::writeDataToFile input buffer invalid");
////            }
//
//            if (!schemaWritten && sinkFormat->getSinkFormat() != FormatTypes::NES_FORMAT) {
//                auto schemaStr = sinkFormat->getFormattedSchema();
//                outputFile.write(schemaStr.c_str(), (int64_t) schemaStr.length());
//                schemaWritten = true;
//            } else if (sinkFormat->getSinkFormat() == FormatTypes::NES_FORMAT) {
//                NES_DEBUG("FileSink::getData: writing schema skipped, not supported for NES_FORMAT");
//            } else {
//                NES_DEBUG("FileSink::getData: schema already written");
//            }
//
////            std::string bufferContent;
////            auto schema = sinkFormat->getSchemaPtr();
////            schema->removeField(AttributeField::create("timestamp", DataTypeFactory::createType(BasicType::UINT64)));
////            bufferContent = Util::printTupleBufferAsCSV(inputBuffer, schema);
//
//
//            auto schema = sinkFormat->getSchemaPtr();
//            std::string repReg = "," + std::to_string(timestamp) + "\n";
//            bufferContent = std::regex_replace(bufferContent, std::regex(R"(\n)"), repReg);
//            schema->addField("timestamp", BasicType::UINT64);
//
//            NES_DEBUG("FileSink::getData: writing to file {} following content {}", filePath, bufferContent);
//            //outputFile.write(bufferContent.c_str(), bufferContent.size());
//            timestampedOutputStrings.push_back(bufferContent);
//
//        }
        for (const auto& bufferContent : receivedBuffers) {
            outputFile.write(bufferContent.c_str(), bufferContent.size());
        }
        outputFile.flush();
    }
}

bool FileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    if (WRITE_ALL_ON_SHUTDOWN) {
        std::string bufferContent;
        auto schema = sinkFormat->getSchemaPtr();
        schema->removeField(AttributeField::create("timestamp", DataTypeFactory::createType(BasicType::UINT64)));
        bufferContent = Util::printTupleBufferAsCSV(inputBuffer, schema);
        receivedBuffers.push_back(bufferContent);
        //arrivalTimestamps.push_back(getTimestamp());
        return true;
    }
    return writeDataToFile(inputBuffer);
}

std::string FileSink::getFilePath() const { return filePath; }

bool FileSink::writeDataToFile(Runtime::TupleBuffer& inputBuffer) {
    std::unique_lock lock(writeMutex);
    NES_DEBUG("FileSink: getSchema medium {} format {} mode {}", toString(), sinkFormat->toString(), this->getAppendAsString());

    if (!inputBuffer) {
        NES_ERROR("FileSink::writeDataToFile input buffer invalid");
        return false;
    }

    if (!schemaWritten && sinkFormat->getSinkFormat() != FormatTypes::NES_FORMAT) {
        auto schemaStr = sinkFormat->getFormattedSchema();
        outputFile.write(schemaStr.c_str(), (int64_t) schemaStr.length());
        schemaWritten = true;
    } else if (sinkFormat->getSinkFormat() == FormatTypes::NES_FORMAT) {
        NES_DEBUG("FileSink::getData: writing schema skipped, not supported for NES_FORMAT");
    } else {
        NES_DEBUG("FileSink::getData: schema already written");
    }

    auto fBuffer = sinkFormat->getFormattedBuffer(inputBuffer);
    NES_DEBUG("FileSink::getData: writing to file {} following content {}", filePath, fBuffer);
    outputFile.write(fBuffer.c_str(), fBuffer.size());
    outputFile.flush();
    return true;
}

bool FileSink::getAppend() const { return append; }

std::string FileSink::getAppendAsString() const {
    if (append) {
        return "APPEND";
    }
    return "OVERWRITE";
}

}// namespace NES
