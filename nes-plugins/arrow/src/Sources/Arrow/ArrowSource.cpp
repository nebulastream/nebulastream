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

#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/ArrowSourceType.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/Arrow/ArrowSource.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace NES {

ArrowSource::ArrowSource(SchemaPtr schema,
                         Runtime::BufferManagerPtr bufferManager,
                         Runtime::QueryManagerPtr queryManager,
                         ArrowSourceTypePtr arrowSourceType,
                         OperatorId operatorId,
                         OriginId originId,
                         size_t numSourceLocalBuffers,
                         GatheringMode gatheringMode,
                         const std::string& physicalSourceName,
                         std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 physicalSourceName,
                 std::move(successors)),
      fileEnded(false), arrowSourceType(arrowSourceType), filePath(arrowSourceType->getFilePath()->getValue()),
      skipHeader(arrowSourceType->getSkipHeader()->getValue()), delimiter(arrowSourceType->getDelimiter()->getValue()),
      numberOfTuplesToProducePerBuffer(arrowSourceType->getNumberOfTuplesToProducePerBuffer()->getValue()) {

    this->numberOfBuffersToProduce = arrowSourceType->getNumberOfBuffersToProduce()->getValue();
    this->gatheringInterval = std::chrono::milliseconds(arrowSourceType->getGatheringInterval()->getValue());
    this->tupleSize = schema->getSchemaSizeInBytes();

    struct Deleter {
        void operator()(const char* ptr) { std::free(const_cast<char*>(ptr)); }
    };
    auto path = std::unique_ptr<const char, Deleter>(const_cast<const char*>(realpath(filePath.c_str(), nullptr)));
    if (path == nullptr) {
        NES_THROW_RUNTIME_ERROR("ArrowSource::ArrowSource: Could not determine absolute pathname: " << filePath.c_str());
    }

    auto openFileStatus = openFile();
    if (!openFileStatus.ok()) {
        NES_THROW_RUNTIME_ERROR("ArrowSource::ArrowSource file error: " << openFileStatus.ToString());
    }

    NES_DEBUG("ArrowSource: Opened Arrow file {}", path.get());
    NES_DEBUG("ArrowSource: tupleSize={} freq={}ms numBuff={} numberOfTuplesToProducePerBuffer={}",
              this->tupleSize,
              this->gatheringInterval.count(),
              this->numberOfBuffersToProduce,
              this->numberOfTuplesToProducePerBuffer);

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
}

std::optional<Runtime::TupleBuffer> ArrowSource::receiveData() {
    NES_TRACE("ArrowSource::receiveData called on  {}", operatorId);
    auto buffer = allocateBuffer();
    fillBuffer(buffer);
    NES_TRACE("ArrowSource::receiveData filled buffer with tuples= {}", buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer.getBuffer();
}

std::string ArrowSource::toString() const {
    std::stringstream ss;
    ss << "ARROW_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count()
       << "ms"
       << " numBuff=" << this->numberOfBuffersToProduce << ")";
    return ss.str();
}

void ArrowSource::fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    // make sure that we have a batch to read
    if (currentRecordBatch == nullptr) {
        readNextBatch();
    }

    // check if file has not ended
    if (this->fileEnded) {
        NES_WARNING("ArrowSource::fillBuffer: but file has already ended");
        return;
    }

    NES_TRACE("ArrowSource::fillBuffer: start at record_batch={} fileSize={}", currentRecordBatch->ToString(), fileSize);

    uint64_t generatedTuplesThisPass = 0;

    // densely pack the buffer
    if (numberOfTuplesToProducePerBuffer == 0) {
        generatedTuplesThisPass = buffer.getCapacity();
    } else {
        generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
        NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < buffer.getBuffer().getBufferSize(),
                        "ArrowSource::fillBuffer: not enough space in tuple buffer to fill tuples in this pass.");
    }
    NES_TRACE("ArrowSource::fillBuffer: fill buffer with #tuples={} of size={}", generatedTuplesThisPass, tupleSize);

    uint64_t tupleCount = 0;

    // Compute how many tuples we can generate from the current batch
    uint64_t tuplesRemainingInCurrentBatch = currentRecordBatch->num_rows() - indexWithinCurrentRecordBatch;

    // Case 1. The number of remaining records in the currentRecordBatch are less than generatedTuplesThisPass. Copy the
    // records from the record batch and keep reading and copying new record batches to fill the buffer.
    if (tuplesRemainingInCurrentBatch < generatedTuplesThisPass) {
        // get the slice of the record batch, slicing is a no copy op
        auto recordBatchSlice = currentRecordBatch->Slice(indexWithinCurrentRecordBatch, tuplesRemainingInCurrentBatch);
        // write the batch to the tuple buffer
        writeRecordBatchToTupleBuffer(tupleCount, buffer, recordBatchSlice);
        tupleCount += tuplesRemainingInCurrentBatch;

        // keep reading record batch and writing to tuple buffer until we have generated generatedTuplesThisPass number
        // of tuples
        while (tupleCount < generatedTuplesThisPass) {
            // read in a new record batch
            readNextBatch();

            // only continue if the file has not ended
            if (this->fileEnded == true)
                break;

            // case when we have to write the whole batch to the tuple buffer
            if ((tupleCount + currentRecordBatch->num_rows()) <= generatedTuplesThisPass) {
                writeRecordBatchToTupleBuffer(tupleCount, buffer, currentRecordBatch);
                tupleCount += currentRecordBatch->num_rows();
                indexWithinCurrentRecordBatch += currentRecordBatch->num_rows();
            }
            // case when we have to write only a part of the batch to the tuple buffer
            else {
                uint64_t lastBatchSize = generatedTuplesThisPass - tupleCount;
                recordBatchSlice = currentRecordBatch->Slice(indexWithinCurrentRecordBatch, lastBatchSize);
                writeRecordBatchToTupleBuffer(tupleCount, buffer, recordBatchSlice);
                tupleCount += lastBatchSize;
                indexWithinCurrentRecordBatch += lastBatchSize;
            }
        }
    }
    // Case 2. The number of remaining records in the currentRecordBatch are greater than generatedTuplesThisPass.
    // simply copy the desired number of tuples from the recordBatch to the tuple buffer
    else {
        // get the slice of the record batch with desired number of tuples
        auto recordBatchSlice = currentRecordBatch->Slice(indexWithinCurrentRecordBatch, generatedTuplesThisPass);
        // write the batch to the tuple buffer
        writeRecordBatchToTupleBuffer(tupleCount, buffer, recordBatchSlice);
        tupleCount += generatedTuplesThisPass;
        indexWithinCurrentRecordBatch += generatedTuplesThisPass;
    }

    buffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    NES_TRACE("ArrowSource::fillBuffer: reading finished read {} tuples", tupleCount);
    NES_TRACE("ArrowSource::fillBuffer: read produced buffer=  {}", Util::printTupleBufferAsCSV(buffer.getBuffer(), schema));
}

SourceType ArrowSource::getType() const { return SourceType::ARROW_SOURCE; }

std::string ArrowSource::getFilePath() const { return filePath; }

const ArrowSourceTypePtr& ArrowSource::getSourceConfig() const { return arrowSourceType; }

arrow::Status ArrowSource::openFile() {
    // the macros initialize the file and recordBatchReader
    // if everything works well return status OK
    // else the macros returns failure
    // if the user specified a .csv, we use a csv reader instead
    if (filePath.ends_with(".csv")) {
        NES_DEBUG("ArrowSource::openFile found CSV file, using CSV parser");
        return openCsvFile();
    }
    ARROW_ASSIGN_OR_RAISE(inputFile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));
    ARROW_ASSIGN_OR_RAISE(recordBatchStreamReader, arrow::ipc::RecordBatchStreamReader::Open(inputFile));
    return arrow::Status::OK();
}

arrow::Status ArrowSource::openCsvFile() {
    ARROW_ASSIGN_OR_RAISE(inputFile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));
    auto read_options = arrow::csv::ReadOptions::Defaults();
    if (skipHeader) {
        read_options.skip_rows = 1;
    }
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = this->delimiter.at(0);
    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    populateOptionsFromSchema(read_options, convert_options);

    auto maybe_reader = arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
                                                          inputFile,
                                                          read_options,
                                                          parse_options,
                                                          convert_options);
    if (!maybe_reader.ok()) {
        // Handle TableReader instantiation error...
        NES_THROW_RUNTIME_ERROR("ArrowSource::ArrowSource CSV reader error: " << maybe_reader.status());
    }
    csvStreamingReader = *maybe_reader;
    return arrow::Status::OK();
}

void ArrowSource::populateOptionsFromSchema(arrow::csv::ReadOptions& readOptions,
                                            arrow::csv::ConvertOptions& convertOptions) {
    auto fields = schema->fields;
    uint64_t numberOfSchemaFields = schema->getSize();
    for (uint64_t schemaFieldIdx = 0; schemaFieldIdx < numberOfSchemaFields; ++schemaFieldIdx) {
        auto nesFieldName = fields[schemaFieldIdx]->getName();
        auto fieldName = nesFieldName.substr(nesFieldName.find('$') + 1);
        readOptions.column_names.push_back(fieldName);
        auto dataType = fields[schemaFieldIdx]->getDataType();
        auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
        if (!physicalType->isBasicType()) {
            NES_THROW_RUNTIME_ERROR("ArrowSource::populateConvertOptionsFromSchema: error inferring types");
        }
        auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicPhysicalType->nativeType) {
            case BasicPhysicalType::NativeType::UINT_8:
                convertOptions.column_types[fieldName] = arrow::uint8();
                break;
            case BasicPhysicalType::NativeType::UINT_16:
                convertOptions.column_types[fieldName] = arrow::uint16();
                break;
            case BasicPhysicalType::NativeType::UINT_32:
                convertOptions.column_types[fieldName] = arrow::uint32();
                break;
            case BasicPhysicalType::NativeType::UINT_64:
                convertOptions.column_types[fieldName] = arrow::uint64();
                break;
            case BasicPhysicalType::NativeType::INT_8:
                convertOptions.column_types[fieldName] = arrow::int8();
                break;
            case BasicPhysicalType::NativeType::INT_16:
                convertOptions.column_types[fieldName] = arrow::int16();
                break;
            case BasicPhysicalType::NativeType::INT_32:
                convertOptions.column_types[fieldName] = arrow::int32();
                break;
            case BasicPhysicalType::NativeType::INT_64:
                convertOptions.column_types[fieldName] = arrow::int64();
                break;
            case BasicPhysicalType::NativeType::FLOAT:
                convertOptions.column_types[fieldName] = arrow::float32();
                break;
            case BasicPhysicalType::NativeType::DOUBLE:
                convertOptions.column_types[fieldName] = arrow::float64();
                break;
            case BasicPhysicalType::NativeType::BOOLEAN:
                convertOptions.column_types[fieldName] = arrow::boolean();
                break;
            case BasicPhysicalType::NativeType::TEXT:
                convertOptions.column_types[fieldName] = arrow::utf8();
                break;
            case BasicPhysicalType::NativeType::CHAR:
            case BasicPhysicalType::NativeType::UNDEFINED:
                NES_THROW_RUNTIME_ERROR("ArrowSource::writeArrowArrayToTupleBuffer: illegal type");
                break;
        }
    }
}

void ArrowSource::readNextBatch() {
    // set the internal index to 0 and read the new batch
    indexWithinCurrentRecordBatch = 0;
    auto readStatus = arrow::Status::OK();
    if (filePath.ends_with(".csv")) {
        readStatus = csvStreamingReader->ReadNext(&currentRecordBatch);
    } else {
        readStatus = recordBatchStreamReader->ReadNext(&currentRecordBatch);
    }

    // check if file has ended
    if (currentRecordBatch == nullptr) {
        this->fileEnded = true;
        NES_TRACE("ArrowSource::readNextBatch: file has ended.");
        return;
    }

    // check if there was some error reading the batch
    if (!readStatus.ok()) {
        NES_THROW_RUNTIME_ERROR("ArrowSource::fillBuffer: error reading recordBatch: " << readStatus.ToString());
    }

    NES_TRACE("ArrowSource::readNextBatch: read the following record batch {}", currentRecordBatch->ToString());
}

// TODO move all logic below to Parser / Format?
// Core Logic in writeRecordBatchToTupleBuffer() and writeArrowArrayToTupleBuffer(): Arrow (and Parquet) format(s)
// is(are) column-oriented. Instead of reconstructing each tuple due to high reconstruction cost, we instead retrieve
// each column from the arrow RecordBatch and then write out the whole column in the tuple buffer.
void ArrowSource::writeRecordBatchToTupleBuffer(uint64_t tupleCount,
                                                Runtime::MemoryLayouts::DynamicTupleBuffer& buffer,
                                                std::shared_ptr<arrow::RecordBatch> recordBatch) {
    auto fields = schema->fields;
    uint64_t numberOfSchemaFields = schema->getSize();
    for (uint64_t columnItr = 0; columnItr < numberOfSchemaFields; columnItr++) {

        // retrieve the arrow column to write from the recordBatch
        auto arrowColumn = recordBatch->column(columnItr);

        // write the column to the tuple buffer
        writeArrowArrayToTupleBuffer(tupleCount, columnItr, buffer, arrowColumn);
    }
}

void ArrowSource::writeArrowArrayToTupleBuffer(uint64_t tupleCountInBuffer,
                                               uint64_t schemaFieldIndex,
                                               Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                               const std::shared_ptr<arrow::Array> arrowArray) {
    if (arrowArray == nullptr) {
        NES_THROW_RUNTIME_ERROR("ArrowSource::writeArrowArrayToTupleBuffer: arrowArray is null.");
    }

    auto fields = schema->fields;
    auto dataType = fields[schemaFieldIndex]->getDataType();
    auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
    uint64_t arrayLength = static_cast<uint64_t>(arrowArray->length());

    // nothing to be done if the array is empty
    if (arrayLength == 0) {
        return;
    }

    try {
        if (physicalType->isBasicType()) {
            auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);

            switch (basicPhysicalType->nativeType) {
                case NES::BasicPhysicalType::NativeType::INT_8: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT8,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT8 data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the int8_t type
                    auto values = std::static_pointer_cast<arrow::Int8Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        int8_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<int8_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_16: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT16,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT16 data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the int16_t type
                    auto values = std::static_pointer_cast<arrow::Int16Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        int16_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<int16_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_32: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT32,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT32 data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the int8_t type
                    auto values = std::static_pointer_cast<arrow::Int32Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        int32_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<int32_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_64: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT64,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT64 data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the int64_t type
                    auto values = std::static_pointer_cast<arrow::Int64Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        int64_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<int64_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_8: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT8,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT8 data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the uint8_t type
                    auto values = std::static_pointer_cast<arrow::UInt8Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        uint8_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<uint8_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_16: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT16,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT16 data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the uint16_t type
                    auto values = std::static_pointer_cast<arrow::UInt16Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        uint16_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<uint16_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_32: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT32,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT32 data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the uint32_t type
                    auto values = std::static_pointer_cast<arrow::UInt32Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        uint32_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<uint32_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_64: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT64,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT64 data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the uint64_t type
                    auto values = std::static_pointer_cast<arrow::UInt64Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        uint64_t value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<uint64_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::FLOAT: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::FLOAT,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent FLOAT data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the float type
                    auto values = std::static_pointer_cast<arrow::FloatArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        float value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<float>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::DOUBLE: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::DOUBLE,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent FLOAT64 data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the float64 type
                    auto values = std::static_pointer_cast<arrow::DoubleArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        double value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<double>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::CHAR: {
                    NES_FATAL_ERROR("ArrowSource::writeArrowArrayToTupleBuffer: type CHAR not supported by Arrow.");
                    throw std::invalid_argument("Arrow does not support CHAR");
                    break;
                }
                case NES::BasicPhysicalType::NativeType::TEXT: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::STRING,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent STRING data types. Type found"
                                    " in IPC file: "
                                        + arrowArray->type()->ToString());

                    // cast the arrow array to the string type
                    auto values = std::static_pointer_cast<arrow::StringArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        auto value = values->GetString(index);

                        auto sizeOfValue = value.size();
                        auto totalSize = sizeOfValue + sizeof(uint32_t);
                        auto childTupleBuffer = allocateVariableLengthField(localBufferManager, totalSize);

                        NES_ASSERT2_FMT(
                            childTupleBuffer.getBufferSize() >= totalSize,
                            "Parser::writeFieldValueToTupleBuffer(): Could not write TEXT field to tuple buffer, there was not "
                            "sufficient space available. Required space: "
                                << totalSize << ", available space: " << childTupleBuffer.getBufferSize());

                        // write out the length and the variable-sized text to the child buffer
                        (*childTupleBuffer.getBuffer<uint32_t>()) = sizeOfValue;
                        std::memcpy(childTupleBuffer.getBuffer() + sizeof(uint32_t), value.data(), sizeOfValue);

                        // attach the child buffer to the parent buffer and write the child buffer index in the
                        // schema field index of the tuple buffer
                        auto childIdx = tupleBuffer.getBuffer().storeChildBuffer(childTupleBuffer);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::BOOLEAN: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::BOOL,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent BOOL data types. Type found"
                                    " in file : "
                                        + arrowArray->type()->ToString()
                                        + ", and type id: " + std::to_string(arrowArray->type()->id()));

                    // cast the arrow array to the boolean type
                    auto values = std::static_pointer_cast<arrow::BooleanArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (uint64_t index = 0; index < arrayLength; ++index) {
                        uint64_t bufferRowIndex = tupleCountInBuffer + index;
                        bool value = values->Value(index);
                        tupleBuffer[bufferRowIndex][schemaFieldIndex].write<bool>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UNDEFINED:
                    NES_FATAL_ERROR("ArrowSource::writeArrowArrayToTupleBuffer: Field Type UNDEFINED");
            }
        } else {
            // We do not support any other ARROW types (such as Lists, Maps, Tensors) yet. We could however later store
            // them in the childBuffers similar to how we store TEXT and push the computation supported by arrow down to
            // them.
            NES_NOT_IMPLEMENTED();
        }
    } catch (const std::exception& e) {
        NES_ERROR("Failed to convert the arrowArray to desired NES data type. Error: {}", e.what());
    }
}

}// namespace NES
