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
#include <Sinks/Formats/ArrowFormat.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <iostream>
#include <regex>
#include <string>
#include <utility>

namespace NES {

SinkMediumTypes FileSink::getSinkMediumType() { return SinkMediumTypes::FILE_SINK; }

FileSink::FileSink(SinkFormatPtr format,
                   Runtime::NodeEnginePtr nodeEngine,
                   uint32_t numOfProducers,
                   const std::string& filePath,
                   bool append,
                   QueryId queryId,
                   QuerySubPlanId querySubPlanId,
                   FaultToleranceType faultToleranceType,
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
    NES_DEBUG("FileSink: open file= {}", filePath);

    // only open the file stream if it is not an arrow file
    if (sinkFormat->getSinkFormat() != FormatTypes::ARROW_IPC_FORMAT) {
        if (!outputFile.is_open()) {
            outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
        }
        NES_ASSERT(outputFile.is_open(), "file is not open");
        NES_ASSERT(outputFile.good(), "file not good");
    }

#ifdef ENABLE_ARROW_BUILD
    if (sinkFormat->getSinkFormat() == FormatTypes::ARROW_IPC_FORMAT) {
        // raise a warning if the file path does not have the streaming "arrows" extension some other system might
        // thus interpret the file differently with different extension
        // the MIME types for arrow files are ".arrow" for file format, and ".arrows" for streaming file format
        // see https://arrow.apache.org/faq/
        if (!(filePath.find(".arrows"))) {
            NES_WARNING("FileSink: An arrow ipc file without '.arrows' extension created as a file sink.");
        }
    }
#endif
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

void FileSink::shutdown() {}

bool FileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
#ifdef ENABLE_ARROW_BUILD
    // handle the case if we write to an arrow file
    if (sinkFormat->getSinkFormat() == FormatTypes::ARROW_IPC_FORMAT) {
        return writeDataToArrowFile(inputBuffer);
    }
#endif//ENABLE_ARROW_BUILD
    // otherwise call the regular function
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
    updateWatermarkCallback(inputBuffer);

    return true;
}

bool FileSink::getAppend() const { return append; }

std::string FileSink::getAppendAsString() const {
    if (append) {
        return "APPEND";
    }
    return "OVERWRITE";
}

#ifdef ENABLE_ARROW_BUILD
bool FileSink::writeDataToArrowFile(Runtime::TupleBuffer& inputBuffer) {
    std::unique_lock lock(writeMutex);

    // preliminary checks
    NES_TRACE("FileSink: getSchema medium {} format {} and mode {}",
              toString(),
              sinkFormat->toString(),
              this->getAppendAsString());

    if (!inputBuffer) {
        NES_ERROR("FileSink::writeDataToArrowFile input buffer invalid");
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

arrow::Status FileSink::openArrowFile(std::shared_ptr<arrow::io::FileOutputStream> arrowFileOutputStream,
                                      std::shared_ptr<arrow::Schema> arrowSchema,
                                      std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowRecordBatchWriter) {
    // the macros initialize the arrowFileOutputStream and arrowRecordBatchWriter
    // if everything goes well return status OK
    // else the macros return failure
    ARROW_ASSIGN_OR_RAISE(arrowFileOutputStream, arrow::io::FileOutputStream::Open(filePath, append));
    ARROW_ASSIGN_OR_RAISE(arrowRecordBatchWriter, arrow::ipc::MakeStreamWriter(arrowFileOutputStream, arrowSchema));
    return arrow::Status::OK();
}
#endif//ENABLE_ARROW_BUILD

}// namespace NES
