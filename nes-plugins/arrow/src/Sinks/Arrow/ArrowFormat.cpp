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

#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Arrow/ArrowFormat.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <brotli/encode.h>
#include <iostream>
#include <utility>

namespace NES {

std::string ArrowFormat::getFormattedSchema() { NES_NOT_IMPLEMENTED(); }

ArrowFormat::ArrowFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager)
    : SinkFormat(std::move(schema), std::move(bufferManager)) {}

std::string ArrowFormat::getFormattedBuffer(Runtime::TupleBuffer&) {
    // since arrow writes it owns file separately along with the schema we do not need it
    NES_NOT_IMPLEMENTED();
}

std::string ArrowFormat::toString() { return "ARROW_IPC_FORMAT"; }

FormatTypes ArrowFormat::getSinkFormat() { return FormatTypes::ARROW_IPC_FORMAT; }

FormatIterator ArrowFormat::getTupleIterator(Runtime::TupleBuffer&) { NES_NOT_IMPLEMENTED(); }

std::vector<std::shared_ptr<arrow::Array>> ArrowFormat::getArrowArrays(Runtime::TupleBuffer& inputBuffer) {
    std::vector<std::shared_ptr<arrow::Array>> arrowArrays;
    uint64_t numberOfFields = schema->fields.size();
    auto numberOfTuples = inputBuffer.getNumberOfTuples();

    // TODO #4082: add support for column layout
    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, inputBuffer.getBufferSize());
    auto testTupleBuffer = Runtime::MemoryLayouts::TestTupleBuffer(layout, inputBuffer);

    // iterate over all fields in the schema to create respective arrow builders
    for (uint64_t columnIndex = 0; columnIndex < numberOfFields; ++columnIndex) {
        auto fieldName = schema->fields[columnIndex]->toString();
        auto attributeField = schema->get(columnIndex);
        auto dataType = attributeField->getDataType();
        auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
        auto fieldSize = physicalType->size();
        uint64_t offset = 0;
        try {
            if (physicalType->isBasicType()) {
                auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                switch (basicPhysicalType->nativeType) {
                    case NES::BasicPhysicalType::NativeType::INT_8: {
                        // create an int8 builder
                        arrow::Int8Builder int8Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            int8_t value = testTupleBuffer[rowIndex][columnIndex].read<int8_t>();
                            auto append = int8Builder.Append(value);
                        }

                        // build the int8Array
                        auto buildArray = int8Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert INT_8 field to arrow array.");
                        }

                        // add the int8Array to the arrowArrays
                        std::shared_ptr<arrow::Array> int8Array = *buildArray;
                        arrowArrays.push_back(int8Array);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::INT_16: {
                        // create an int16 builder
                        arrow::Int16Builder int16Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            int16_t value = testTupleBuffer[rowIndex][columnIndex].read<int16_t>();
                            auto append = int16Builder.Append(value);
                        }

                        // build the int16Array
                        auto buildArray = int16Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert INT_16 field to arrow array.");
                        }

                        // add the int16Array to the arrowArrays
                        std::shared_ptr<arrow::Array> int16Array = *buildArray;
                        arrowArrays.push_back(int16Array);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::INT_32: {
                        // create an int32 builder
                        arrow::Int32Builder int32Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            int32_t value = testTupleBuffer[rowIndex][columnIndex].read<int32_t>();
                            auto append = int32Builder.Append(value);
                        }

                        // build the int32Array
                        auto buildArray = int32Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert INT_32 field to arrow array.");
                        }

                        // add the int32Array to the arrowArrays
                        std::shared_ptr<arrow::Array> int32Array = *buildArray;
                        arrowArrays.push_back(int32Array);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::INT_64: {
                        // create an int64 builder
                        arrow::Int64Builder int64Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            int64_t value = testTupleBuffer[rowIndex][columnIndex].read<int64_t>();
                            auto append = int64Builder.Append(value);
                        }

                        // build the int64Array
                        auto buildArray = int64Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert INT_64 field to arrow array.");
                        }

                        // add the int64Array to the arrowArrays
                        std::shared_ptr<arrow::Array> int64Array = *buildArray;
                        arrowArrays.push_back(int64Array);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::UINT_8: {
                        // create an uint8 builder
                        arrow::UInt8Builder uint8Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            uint8_t value = testTupleBuffer[rowIndex][columnIndex].read<uint8_t>();
                            auto append = uint8Builder.Append(value);
                        }

                        // build the uint8Array
                        auto buildArray = uint8Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert UINT_8 field to arrow array.");
                        }

                        // add the uint8Array to the arrowArrays
                        std::shared_ptr<arrow::Array> uint8Array = *buildArray;
                        arrowArrays.push_back(uint8Array);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::UINT_16: {
                        // create an uint16 builder
                        arrow::UInt16Builder uint16Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            uint16_t value = testTupleBuffer[rowIndex][columnIndex].read<uint16_t>();
                            auto append = uint16Builder.Append(value);
                        }

                        // build the uint16Array
                        auto buildArray = uint16Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert UINT_16 field to arrow array.");
                        }

                        // add the uint16Array to the arrowArrays
                        std::shared_ptr<arrow::Array> uint16Array = *buildArray;
                        arrowArrays.push_back(uint16Array);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::UINT_32: {
                        // create an uint32 builder
                        arrow::UInt32Builder uint32Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            uint32_t value = testTupleBuffer[rowIndex][columnIndex].read<uint32_t>();
                            auto append = uint32Builder.Append(value);
                        }

                        // build the uint32Array
                        auto buildArray = uint32Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert UINT_32 field to arrow array.");
                        }

                        // add the uint32Array to the arrowArrays
                        std::shared_ptr<arrow::Array> uint32Array = *buildArray;
                        arrowArrays.push_back(uint32Array);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::UINT_64: {
                        // create an uint64 builder
                        arrow::UInt64Builder uint64Builder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            uint64_t value = testTupleBuffer[rowIndex][columnIndex].read<uint64_t>();
                            auto append = uint64Builder.Append(value);
                        }

                        // build the uint64Array
                        auto buildArray = uint64Builder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert UINT_64 field to arrow array.");
                        }

                        // add the uint64Array to the arrowArrays
                        std::shared_ptr<arrow::Array> uint64Array = *buildArray;
                        arrowArrays.push_back(uint64Array);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::FLOAT: {
                        // create a float builder
                        arrow::FloatBuilder floatBuilder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            float value = testTupleBuffer[rowIndex][columnIndex].read<float>();
                            auto append = floatBuilder.Append(value);
                        }

                        // build the floatArray
                        auto buildArray = floatBuilder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert FLOAT field to arrow array.");
                        }

                        // add the floatArray to the arrowArrays
                        std::shared_ptr<arrow::Array> floatArray = *buildArray;
                        arrowArrays.push_back(floatArray);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::DOUBLE: {
                        // create a double builder
                        arrow::DoubleBuilder doubleBuilder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            double value = testTupleBuffer[rowIndex][columnIndex].read<double>();
                            auto append = doubleBuilder.Append(value);
                        }

                        // build the doubleArray
                        auto buildArray = doubleBuilder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert DOUBLE field to arrow array.");
                        }

                        // add the doubleArray to the arrowArrays
                        std::shared_ptr<arrow::Array> doubleArray = *buildArray;
                        arrowArrays.push_back(doubleArray);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::TEXT: {
                        arrow::StringBuilder stringBuilder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            std::string str;
                            auto* buffer = inputBuffer.getBuffer<char>();
                            auto indexInBuffer = buffer + offset + rowIndex * schema->getSchemaSizeInBytes();

                            // read the child buffer index from the tuple buffer
                            Runtime::TupleBuffer::NestedTupleBufferKey childIdx =
                                *reinterpret_cast<uint32_t const*>(indexInBuffer);

                            // retrieve the child buffer from the tuple buffer
                            auto childTupleBuffer = inputBuffer.loadChildBuffer(childIdx);

                            // retrieve the size of the variable-length field from the child buffer
                            uint32_t sizeOfTextField = *(childTupleBuffer.getBuffer<uint32_t>());

                            // build the string
                            if (sizeOfTextField > 0) {
                                auto begin = childTupleBuffer.getBuffer() + sizeof(uint32_t);
                                std::string deserialized(begin, begin + sizeOfTextField);
                                str = std::move(deserialized);
                            }

                            else {
                                NES_WARNING("ArrowFormat::getArrowArrays: Variable-length field could not be read."
                                            "Invalid size in the variable-length TEXT field. Adding an empty string.")
                            }

                            auto append = stringBuilder.Append(str);
                        }

                        // build the stringArray
                        auto buildArray = stringBuilder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert TEXT field to arrow array.");
                        }

                        // add the stringArray to the arrowArrays
                        std::shared_ptr<arrow::Array> stringArray = *buildArray;
                        arrowArrays.push_back(stringArray);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::BOOLEAN: {
                        // create a boolean builder
                        arrow::BooleanBuilder booleanBuilder;

                        // iterate over all values in the column and add values to the builder
                        for (uint64_t rowIndex = 0; rowIndex < numberOfTuples; ++rowIndex) {
                            bool value = testTupleBuffer[rowIndex][columnIndex].read<bool>();
                            auto append = booleanBuilder.Append(value);
                        }

                        // build the booleanArray
                        auto buildArray = booleanBuilder.Finish();
                        if (!buildArray.ok()) {
                            NES_FATAL_ERROR("ArrowFormat::getArrowArrays: could not convert BOOLEAN field to arrow array.");
                        }

                        // add the booleanArray to the arrowArrays
                        std::shared_ptr<arrow::Array> booleanArray = *buildArray;
                        arrowArrays.push_back(booleanArray);
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::CHAR: {
                        NES_FATAL_ERROR("ArrowFormat::getArrowArrays: type CHAR not supported by Arrow.");
                        throw std::invalid_argument("Arrow does not support CHAR");
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::UNDEFINED: {
                        NES_FATAL_ERROR("ArrowFormat::getArrowArrays: Field Type UNDEFINED");
                        break;
                    }
                }
            }
        } catch (const std::exception& e) {
            NES_ERROR("ArrowFormat::getArrowArrays: Failed to convert the arrowArray to desired NES data type. "
                      "Error: {}",
                      e.what());
        }
        offset += fieldSize;
    }

    return arrowArrays;
}

std::shared_ptr<arrow::Schema> ArrowFormat::getArrowSchema() {
    std::vector<std::shared_ptr<arrow::Field>> arrowFields;
    std::shared_ptr<arrow::Schema> arrowSchema;
    uint64_t numberOfFields = schema->fields.size();

    // create arrow fields and add them to the field vector
    for (uint64_t i = 0; i < numberOfFields; i++) {
        auto fieldName = schema->fields[i]->toString();
        auto attributeField = schema->get(i);
        auto dataType = attributeField->getDataType();
        auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
        try {
            if (physicalType->isBasicType()) {
                auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                switch (basicPhysicalType->nativeType) {
                    case NES::BasicPhysicalType::NativeType::INT_8: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::int8()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::INT_16: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::int16()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::INT_32: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::int32()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::INT_64: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::int64()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::UINT_8: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::uint8()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::UINT_16: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::uint16()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::UINT_32: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::uint32()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::UINT_64: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::uint64()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::FLOAT: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::float32()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::DOUBLE: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::float64()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::TEXT: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::utf8()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::BOOLEAN: {
                        arrowFields.push_back(arrow::field(fieldName, arrow::boolean()));
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::CHAR: {
                        NES_FATAL_ERROR("ArrowFormat::getArrowSchema: type CHAR not supported by Arrow.");
                        throw std::invalid_argument("Arrow does not support CHAR");
                        break;
                    }
                    case NES::BasicPhysicalType::NativeType::UNDEFINED: {
                        NES_FATAL_ERROR("ArrowFormat::getArrowSchema: Field Type UNDEFINED");
                        break;
                    }
                }
            }
        } catch (const std::exception& e) {
            NES_ERROR("Failed to convert the arrowArray to desired NES data type. Error: {}", e.what());
        }
    }

    // build arrow schema
    arrowSchema = arrow::schema(arrowFields);
    return arrowSchema;
}

}// namespace NES