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

#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Arrow/ArrowFileSink.hpp>
#include <Sinks/Arrow/ArrowFormat.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <iostream>
#include <regex>
#include <string>
#include <utility>

namespace NES {

SinkMediumTypes ArrowFileSink::getSinkMediumType() { return SinkMediumTypes::FILE_SINK; }

ArrowFileSink::ArrowFileSink(SinkFormatPtr format,
                             Runtime::NodeEnginePtr nodeEngine,
                             uint32_t numOfProducers,
                             const std::string& filePath,
                             bool append,
                             QueryId queryId,
                             QuerySubPlanId querySubPlanId,
                             uint64_t numberOfOrigins)
    : SinkMedium(std::move(format),
                 std::move(nodeEngine),
                 numOfProducers,
                 queryId,
                 querySubPlanId,
                 numberOfOrigins) {
    this->filePath = filePath;
    this->append = append;
    if (!append) {
        if (std::filesystem::exists(filePath.c_str())) {
            bool success = std::filesystem::remove(filePath.c_str());
            NES_ASSERT2_FMT(success, "cannot remove file " << filePath.c_str());
        }
    }
    NES_DEBUG("ArrowFileSink: open file= {}", filePath);

    if (sinkFormat->getSinkFormat() == FormatTypes::ARROW_IPC_FORMAT) {
        // raise a warning if the file path does not have the streaming "arrows" extension some other system might
        // thus interpret the file differently with different extension
        // the MIME types for arrow files are ".arrow" for file format, and ".arrows" for streaming file format
        // see https://arrow.apache.org/faq/
        if (!(filePath.find(".arrows"))) {
            NES_WARNING("ArrowFileSink: An arrow ipc file without '.arrows' extension created as a file sink.");
        }
    }
}

ArrowFileSink::~ArrowFileSink() {
    NES_DEBUG("~ArrowFileSink: close file={}", filePath);
    outputFile.close();
}

std::string ArrowFileSink::toString() const {
    std::stringstream ss;
    ss << "ArrowFileSink(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void ArrowFileSink::setup() {}

void ArrowFileSink::shutdown() {}

bool ArrowFileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    return writeDataToArrowFile(inputBuffer);
}

std::string ArrowFileSink::getFilePath() const { return filePath; }

bool ArrowFileSink::getAppend() const { return append; }

std::string ArrowFileSink::getAppendAsString() const {
    if (append) {
        return "APPEND";
    }
    return "OVERWRITE";
}

bool ArrowFileSink::writeDataToArrowFile(Runtime::TupleBuffer& inputBuffer) {
    std::unique_lock lock(writeMutex);

    // preliminary checks
    NES_TRACE("ArrowFileSink: getSchema medium {} format {} and mode {}",
              toString(),
              sinkFormat->toString(),
              this->getAppendAsString());

    if (!inputBuffer) {
        NES_ERROR("ArrowFileSink::writeDataToArrowFile input buffer invalid");
        return false;
    }

    // make arrow schema
    auto arrowFormat = std::dynamic_pointer_cast<ArrowFormat>(sinkFormat);
    std::shared_ptr<arrow::Schema> arrowSchema = arrowFormat->getArrowSchema();

    // open the arrow ipc file
    std::shared_ptr<arrow::io::FileOutputStream> outfileArrow;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
    arrow::Status openStatus = openArrowFile(outfileArrow, arrowSchema, arrowWriter);

    if (!openStatus.ok()) {
        return false;
    }

    // get arrow arrays from tuple buffer
    std::vector<std::shared_ptr<arrow::Array>> arrowArrays = arrowFormat->getArrowArrays(inputBuffer);

    // make a record batch
    std::shared_ptr<arrow::RecordBatch> recordBatch =
        arrow::RecordBatch::Make(arrowSchema, arrowArrays[0]->length(), arrowArrays);

    // write the record batch
    auto write = arrowWriter->WriteRecordBatch(*recordBatch);

    // close the writer
    auto close = arrowWriter->Close();

    return true;
}

arrow::Status ArrowFileSink::openArrowFile(std::shared_ptr<arrow::io::FileOutputStream> arrowFileOutputStream,
                                           std::shared_ptr<arrow::Schema> arrowSchema,
                                           std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowRecordBatchWriter) {
    // the macros initialize the arrowFileOutputStream and arrowRecordBatchWriter
    // if everything goes well return status OK
    // else the macros return failure
    ARROW_ASSIGN_OR_RAISE(arrowFileOutputStream, arrow::io::FileOutputStream::Open(filePath, append));
    ARROW_ASSIGN_OR_RAISE(arrowRecordBatchWriter, arrow::ipc::MakeStreamWriter(arrowFileOutputStream, arrowSchema));
    return arrow::Status::OK();
}

}// namespace NES
