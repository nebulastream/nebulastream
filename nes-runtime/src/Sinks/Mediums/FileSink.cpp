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

#include <Exceptions/TaskExecutionException.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <iostream>
#include <string>
#include <utility>

namespace NES {

SinkMediumTypes FileSink::getSinkMediumType() { return SinkMediumTypes::FILE_SINK; }

FileSink::FileSink(SinkFormatPtr format,
                   Runtime::NodeEnginePtr nodeEngine,
                   uint32_t numOfProducers,
                   const std::string& filePath,
                   bool append,
                   SharedQueryId sharedQueryId,
                   DecomposedQueryId decomposedQueryId,
                   DecomposedQueryPlanVersion decomposedQueryVersion,
                   uint64_t numberOfOrigins)
    : SinkMedium(std::move(format),
                 std::move(nodeEngine),
                 numOfProducers,
                 sharedQueryId,
                 decomposedQueryId,
                 decomposedQueryVersion,
                 numberOfOrigins),
      filePath(filePath), append(append) {}

std::string FileSink::toString() const {
    std::stringstream ss;
    ss << "FileSink(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void FileSink::setup() {
    NES_DEBUG("Setting up file sink; filePath={}, schema={}, sinkFormat={}, append={}",
              filePath,
              sinkFormat->getSchemaPtr()->toString(),
              sinkFormat->toString(),
              append);
    // Remove an existing file unless the append mode is APPEND.
    if (!append) {
        if (std::filesystem::exists(filePath.c_str())) {
            std::error_code ec;
            if (!std::filesystem::remove(filePath.c_str(), ec)) {
                NES_ERROR("Could not remove existing output file: filePath={} ", filePath);
                isOpen = false;
                return;
            }
        }
    } else {
        if (std::filesystem::exists(filePath.c_str())) {
            // Skip writing the schema to the file as we are appending to an existing file.
            schemaWritten = true;
        }
    }

    // Open the file stream
    if (!outputFile.is_open()) {
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
    }
    isOpen = outputFile.is_open() && outputFile.good();
    if (!isOpen) {
        NES_ERROR("Could not open output file; filePath={}, is_open()={}, good={}",
                  filePath,
                  outputFile.is_open(),
                  outputFile.good());
    }
}

void FileSink::shutdown() {
    NES_WARNING("Closing file sink, filePath={}", filePath);
    outputFile.close();
}

bool FileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    // Stop execution if the file could not be opened during setup.
    // This results in ExecutionResult::Error for the task.
    if (!isOpen) {
        NES_DEBUG("The output file could not be opened during setup of the file sink.");
        return false;
    }
    std::unique_lock lock(writeMutex);

    if (!inputBuffer) {
        NES_ERROR("Invalid input buffer");
        return false;
    }

    if (!schemaWritten && sinkFormat->getSinkFormat() != FormatTypes::NES_FORMAT) {
        NES_DEBUG("Writing schema to file sink; filePath = {}, schema = {}, sinkFormat = {}",
                  filePath,
                  sinkFormat->getSchemaPtr()->toString(),
                  sinkFormat->toString());
        auto schemaStr = sinkFormat->getFormattedSchema();
        outputFile.write(schemaStr.c_str(), (int64_t) schemaStr.length());
        schemaWritten = true;
    } else if (sinkFormat->getSinkFormat() == FormatTypes::NES_FORMAT) {
        NES_DEBUG("Writing the schema is not supported for NES_FORMAT");
    } else {
        NES_DEBUG("Schema already written");
    }

    auto fBuffer = sinkFormat->getFormattedBuffer(inputBuffer);
    NES_DEBUG("Writing tuples to file sink; decomposed query id={} filePath={}, fBuffer={}", decomposedQueryId, filePath, fBuffer);
    outputFile.write(fBuffer.c_str(), fBuffer.size());
    outputFile.flush();
    return true;
}

}// namespace NES
