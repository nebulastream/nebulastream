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

#ifdef ENABLE_ARROW_BUILD

#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/ArrowSource.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#define ARROW_READ_NOT_OK(s)                                                \
    do {                                                                    \
        arrow::Status _s = (s);                                             \
        if (!_s.ok()) {                                                     \
            throw USER_EXCEPTION(                                           \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION)          \
                    << _s.ToString().c_str();                               \
        }                                                                   \
    } while (0)

#define THROW_NOT_OK_FILE(s)                                            \
    do {                                                                \
        arrow::Status _s = (s);                                         \
        if (!_s.ok()) {                                                 \
            throw USER_EXCEPTION(                                       \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)       \
                    << _s.ToString().c_str() << (int)_s.code();         \
        }                                                               \
    } while(0)

namespace NES {

ArrowSource::ArrowSource(SchemaPtr schema,
                         Runtime::BufferManagerPtr bufferManager,
                         Runtime::QueryManagerPtr queryManager,
                         ArrowSourceTypePtr arrowSourceType,
                         OperatorId operatorId,
                         OriginId originId,
                         size_t numSourceLocalBuffers,
                         GatheringMode gatheringMode,
                         std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(successors)),
      fileEnded(false), arrowSourceType(arrowSourceType), filePath(arrowSourceType->getFilePath()->getValue()),
      numberOfTuplesToProducePerBuffer(arrowSourceType->getNumberOfTuplesToProducePerBuffer()->getValue()) {

  this->numberOfBuffersToProduce = arrowSourceType->getNumberOfBuffersToProduce()->getValue();
  this->gatheringInterval = std::chrono::milliseconds(arrowSourceType->getGatheringInterval()->getValue());
  this->tupleSize = schema->getSchemaSizeInBytes();

  struct Deleter {
    void operator()(const char *ptr) { std::free(const_cast<char *>(ptr)); }
  };
  auto path = std::unique_ptr<const char, Deleter>(const_cast<const char *>(realpath(filePath.c_str(), nullptr)));
  if (path == nullptr) {
    NES_THROW_RUNTIME_ERROR("ArrowSource::ArrowSource: Could not determine absolute pathname: " << filePath.c_str());
  }

  auto openFileStatus = openFile();

  if(!openFileStatus.ok()) {
      NES_THROW_RUNTIME_ERROR("ArrowSource::ArrowSource file error: " << openFileStatus.ToString());
  }

  NES_DEBUG2("ArrowSource: Opened Arrow IPC file {}", path.get());
  NES_DEBUG2("ArrowSource: tupleSize={} freq={}ms numBuff={} numberOfTuplesToProducePerBuffer={}",
             this->tupleSize,
             this->gatheringInterval.count(),
             this->numberOfBuffersToProduce,
             this->numberOfTuplesToProducePerBuffer);

  DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
  for (const AttributeFieldPtr &field : schema->fields) {
    auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
    physicalTypes.push_back(physicalField);
  }
}

std::optional<Runtime::TupleBuffer> ArrowSource::receiveData() {
  NES_TRACE2("ArrowSource::receiveData called on  {}", operatorId);
  auto buffer = allocateBuffer();
  fillBuffer(buffer);
  NES_TRACE2("ArrowSource::receiveData filled buffer with tuples= {}", buffer.getNumberOfTuples());

  if (buffer.getNumberOfTuples() == 0) {
    return std::nullopt;
  }
  return buffer.getBuffer();
}

std::string ArrowSource::toString() const {
  std::stringstream ss;
  ss << "ARROW_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq="
     << this->gatheringInterval.count()
     << "ms"
     << " numBuff=" << this->numberOfBuffersToProduce << ")";
  return ss.str();
}

void ArrowSource::fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer &buffer) {
  NES_TRACE2("ArrowSource::fillBuffer: start at record_batch={} fileSize={}", currentRecordBatch->ToString(), fileSize);
  if (this->fileEnded) {
    NES_WARNING2("ArrowSource::fillBuffer: but file has already ended");
    return;
  }

  uint64_t generatedTuplesThisPass = 0;
  // densely pack the buffer
  if (numberOfTuplesToProducePerBuffer == 0) {
    generatedTuplesThisPass = buffer.getCapacity();
  } else {
    generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
    NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < buffer.getBuffer().getBufferSize(), "ArrowSource::fillBuffer: Wrong parameters");
  }
  NES_TRACE2("ArrowSource::fillBuffer: fill buffer with #tuples={} of size={}", generatedTuplesThisPass, tupleSize);

  std::string line;
  uint64_t tupleCount = 0;

  // make sure that we have a batch to read
  if(currentRecordBatch == nullptr) {
      auto readBatchStatus = readNextBatch();
      NES_THROW_RUNTIME_ERROR("ArrowSource::fillBuffer: error reading recordBatch: " << readBatchStatus.ToString());
  }

  // Compute how many tuples we can generate from the current batch
  uint64_t tuplesRemainingInCurrentBatch = currentRecordBatch->num_rows() - indexWithinCurrentRecordBatch;

  // Case 1. The number of remaining records in the currentRecordBatch are less generatedTuplesThisPass. Copy the
  // records from the record batch and read in a new record batch to fill the rest of the buffer
  if(tuplesRemainingInCurrentBatch < generatedTuplesThisPass) {
      // get the slice of the record batch, this is a no copy op
      auto recordBatchSlice = currentRecordBatch->Slice(indexWithinCurrentRecordBatch, tuplesRemainingInCurrentBatch);
      // write the batch to the tuple buffer
      writeRecordBatchToTupleBuffer(buffer, recordBatchSlice);

      // read in a new record batch
      auto readBatchStatus = readNextBatch();
      indexWithinCurrentRecordBatch = 0;

      // get the slice of the record batch with remaining tuples to generate
      recordBatchSlice = currentRecordBatch->Slice(indexWithinCurrentRecordBatch,
                                                   generatedTuplesThisPass - tuplesRemainingInCurrentBatch);
      indexWithinCurrentRecordBatch += generatedTuplesThisPass - tuplesRemainingInCurrentBatch;
      // write the batch to the tuple buffer
      writeRecordBatchToTupleBuffer(buffer, recordBatchSlice);
  }
  // Case 2. The number of remaining records in the currentRecordBatch are greater than generatedTuplesThisPass.
  // simply copy the desired number of tuples from the recordBatch to the tuple buffer
  else {
      // get the slice of the record batch with desired number of tuples
      auto recordBatchSlice = currentRecordBatch->Slice(indexWithinCurrentRecordBatch, generatedTuplesThisPass);
      // write the batch to the tuple buffer
      writeRecordBatchToTupleBuffer(buffer, recordBatchSlice);
  }

  buffer.setNumberOfTuples(tupleCount);
  generatedTuples += tupleCount;
  generatedBuffers++;
  NES_TRACE2("ArrowSource::fillBuffer: reading finished read {} tuples",
             tupleCount);
  NES_TRACE2("ArrowSource::fillBuffer: read produced buffer=  {}",
             Util::printTupleBufferAsCSV(buffer.getBuffer(), schema));
}

SourceType ArrowSource::getType() const { return SourceType::ARROW_SOURCE; }

std::string ArrowSource::getFilePath() const { return filePath; }

const ArrowSourceTypePtr &ArrowSource::getSourceConfig() const { return arrowSourceType; }

arrow::Status ArrowSource::openFile() {
    // the macros initialize the file and recordBatchReader
    // if everything works well return status OK
    // else the macros returns failure
    ARROW_ASSIGN_OR_RAISE(inputFile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));
    ARROW_ASSIGN_OR_RAISE(recordBatchStreamReader, arrow::ipc::RecordBatchStreamReader::Open(inputFile));
    return arrow::Status::OK();
}

arrow::Status ArrowSource::readNextBatch() {
    //set the index to 0 and read the new batch
    indexWithinCurrentRecordBatch = 0;
    return recordBatchStreamReader->ReadNext(&currentRecordBatch);
}

// TODO move all logic below to Parser / Format?
void ArrowSource::writeRecordBatchToTupleBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer &buffer,
                                                std::shared_ptr<arrow::RecordBatch> recordBatch) {
    auto fields = schema->fields;
    uint64_t numberOfSchemaFields = schema->getSize();
    for (uint64_t columnItr = 0; columnItr < numberOfSchemaFields; columnItr++) {

        // retrieve the arrow column to write from the recordBatch
        auto arrowColumn = recordBatch->column(columnItr);

        // write the column to the tuple buffer
        writeArrowArrayToTupleBuffer(columnItr, buffer, arrowColumn);
    }
}

void ArrowSource::writeArrowArrayToTupleBuffer(uint64_t schemaFieldIndex,
                                               Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer,
                                               const std::shared_ptr<arrow::Array> arrowArray) {
    if(arrowArray == nullptr) {
        NES_THROW_RUNTIME_ERROR("ArrowSource::writeArrowArrayToTupleBuffer: arrowArray is null.");
    }

    auto fields = schema->fields;
    auto dataType = fields[schemaFieldIndex]->getDataType();
    auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
    auto arrayLength = arrowArray->length();

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
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT8 data types.");

                    // cast the arrow array to the int8_t type
                    auto values = std::static_pointer_cast<arrow::Int8Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        int8_t value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<int8_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_16: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT16,
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT16 data types.");

                    // cast the arrow array to the int16_t type
                    auto values = std::static_pointer_cast<arrow::Int16Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        int16_t value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<int16_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_32: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT32,
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT32 data types.");

                    // cast the arrow array to the int8_t type
                    auto values = std::static_pointer_cast<arrow::Int32Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        int32_t value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<int32_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_64: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::INT64,
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent INT64 data types.");

                    // cast the arrow array to the int64_t type
                    auto values = std::static_pointer_cast<arrow::Int64Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        int64_t value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<int64_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_8: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT8,
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT8 data types.");

                    // cast the arrow array to the uint8_t type
                    auto values = std::static_pointer_cast<arrow::UInt8Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        uint8_t value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<uint8_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_16: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT16,
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT16 data types.");

                    // cast the arrow array to the uint16_t type
                    auto values = std::static_pointer_cast<arrow::UInt16Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        uint16_t value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<uint16_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_32: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT32,
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT32 data types.");

                    // cast the arrow array to the uint32_t type
                    auto values = std::static_pointer_cast<arrow::UInt32Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        uint32_t value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<uint32_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_64: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::UINT64,
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent UINT64 data types.");

                    // cast the arrow array to the uint64_t type
                    auto values = std::static_pointer_cast<arrow::UInt64Array>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        uint64_t value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<uint64_t>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::FLOAT: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::FLOAT,
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent FLOAT data types.");

                    // cast the arrow array to the uint8_t type
                    auto values = std::static_pointer_cast<arrow::FloatArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        float value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<float>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::DOUBLE: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::DOUBLE,
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent FLOAT64 data types.");

                    // cast the arrow array to the float64 type
                    auto values = std::static_pointer_cast<arrow::FloatArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        double value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<double>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::CHAR: {
                    NES_FATAL_ERROR2("ArrowSource::writeArrowArrayToTupleBuffer: type CHAR not supported by Arrow.");
                    throw std::invalid_argument("Arrow does not support CHAR");
                    break;
                }
                case NES::BasicPhysicalType::NativeType::TEXT: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::STRING,
                       "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent STRING data types.");

                    // cast the arrow array to the string type
                    auto values = std::static_pointer_cast<arrow::StringArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        //std::string value = values->Value(index);
                        auto value = values->Value(index);

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
                        tupleBuffer[index][schemaFieldIndex].write<Runtime::TupleBuffer::NestedTupleBufferKey>(childIdx);

                        break;
                    }
                }
                case NES::BasicPhysicalType::NativeType::BOOLEAN: {
                    NES_ASSERT2_FMT(arrowArray->type()->id() == arrow::Type::type::BOOL,
                                    "ArrowSource::writeArrowArrayToTupleBuffer: inconsistent BOOL data types.");

                    // cast the arrow array to the boolean type
                    auto values = std::static_pointer_cast<arrow::BooleanArray>(arrowArray);

                    // write all values to the tuple buffer
                    for (int64_t index = 0; index < arrayLength; ++index) {
                        bool value = values->Value(index);
                        tupleBuffer[index][schemaFieldIndex].write<bool>(value);
                    }
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UNDEFINED:
                    NES_FATAL_ERROR2("ArrowSource::writeArrowArrayToTupleBuffer: Field Type UNDEFINED");
            }
        } else {
            // We do not support any other ARROW types (such as Lists, Maps, Tensors) yet. We could however later store
            // them in the childBuffers similar to how we store TEXT and push the computation supported by arrow down to
            // them.
        }
    } catch (const std::exception& e) {
        NES_ERROR2("Failed to convert the arrowArray to desired NES data type. Error: {}", e.what());
    }
}

}// namespace NES

#endif// ENABLE_ARROW_BUILD
