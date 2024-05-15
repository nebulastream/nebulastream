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
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <iostream>
#include <string>
#include <utility>
#include <Exceptions/TaskExecutionException.hpp>
#include <Runtime/QueryManager.hpp>

namespace NES {

SinkMediumTypes FileSink::getSinkMediumType() { return SinkMediumTypes::FILE_SINK; }

FileSink::FileSink(SinkFormatPtr format,
                   Runtime::NodeEnginePtr nodeEngine,
                   uint32_t numOfProducers,
                   const std::string& filePath,
                   bool append,
                   SharedQueryId sharedQueryId,
                   DecomposedQueryPlanId decomposedQueryPlanId,
                   uint64_t numberOfOrigins)
    : SinkMedium(std::move(format),
                 std::move(nodeEngine),
                 numOfProducers,
                 sharedQueryId,
                 decomposedQueryPlanId,
                 numberOfOrigins),
      filePath(filePath), append(append) { }

FileSink::~FileSink() { }

std::string FileSink::toString() const {
    std::stringstream ss;
    ss << "FileSink(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void FileSink::setup() {
    if (!append) {
        if (std::filesystem::exists(filePath.c_str())) {
            std::error_code ec;
            if (!std::filesystem::remove(filePath.c_str(), ec)) {
                NES_ERROR("Could not remove existing output file: filePath = {} ", filePath);
                isOpen = false;
                return;
            }
        }
    }
    NES_DEBUG("FileSink: open file= {}", filePath);

    // open the file stream
    if (!outputFile.is_open()) {
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
    }
    isOpen = outputFile.is_open() && outputFile.good();
    if (!isOpen) {
        NES_ERROR("Could not open output file; filePath = {}, is_open() = {}, good = {}", filePath, outputFile.is_open(), outputFile.good());
    }
}

void FileSink::shutdown() {
    NES_DEBUG("~FileSink: close file={}", filePath);
    outputFile.close();
}

bool FileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) { return writeDataToFile(inputBuffer); }

std::string FileSink::getFilePath() const { return filePath; }

bool FileSink::writeDataToFile(Runtime::TupleBuffer& inputBuffer) {
    // Stop execution if the file could not be opened during setup.
    // This results in ExecutionResult::Error for the task.
    if (!isOpen) {
        return false;
    }
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
