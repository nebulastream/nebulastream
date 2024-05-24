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
#include <API/Schema.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Operators/Arrow/ArrowFieldReader.hpp>
#include <Execution/Operators/Arrow/RecordBufferWrapper.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <arrow/api.h>
#include <utility>

namespace NES::Runtime::Execution::Operators {

AbstractArrowFieldReader::AbstractArrowFieldReader(const uint64_t fieldIndex, const Record::RecordFieldIdentifier& fieldName)
    : fieldIndex(fieldIndex), fieldName(fieldName) {}

template<typename ArrowType>
ArrowFieldReader<ArrowType>::ArrowFieldReader(const uint64_t fieldIndex, const Record::RecordFieldIdentifier& fieldName)
    : AbstractArrowFieldReader(fieldIndex, fieldName) {}

template<typename T>
void* getRawArrowColumn(void* ptr, uint64_t index) {
    auto wrapper = (RecordBufferWrapper*) ptr;
    auto column = wrapper->batch->column(index);
    return (void*) std::static_pointer_cast<T>(column)->raw_values();
}

template<typename T>
void* getArrowColumn(void* ptr, uint64_t index) {
    auto wrapper = (RecordBufferWrapper*) ptr;
    auto column = wrapper->batch->column(index);
    return (void*) std::static_pointer_cast<T>(column).get();
}

template<typename T>
auto getArrayValue(void* ptr, uint64_t index) {
    auto array = (T*) ptr;
    return array->Value(index);
}

TextValue* getStringValue(void* ptr, uint64_t index) {
    auto array = (arrow::StringArray*) ptr;
    auto string = array->GetString(index);
    return TextValue::create(string);
}

template<typename ArrowType>
Value<> ArrowFieldReader<ArrowType>::getColumn(const Value<NES::Nautilus::MemRef>& recordBatch) {
    if constexpr (HasRawValues<ArrowType>) {
        return FunctionCall<>("getRawArrowColumn" + std::string(typeid(ArrowType).name()),
                              getRawArrowColumn<ArrowType>,
                              recordBatch,
                              Value<UInt64>(fieldIndex));
    } else {
        return FunctionCall<>("getColumn" + std::string(typeid(ArrowType).name()),
                              getArrowColumn<ArrowType>,
                              recordBatch,
                              Value<UInt64>(fieldIndex));
    }
}

template<typename ArrowType>
Value<> ArrowFieldReader<ArrowType>::getValue(const Value<NES::Nautilus::MemRef>& column, const Value<UInt64>& index) {
    if constexpr (HasRawValues<ArrowType>) {
        using cType = typename ArrowType::value_type;
        auto offset = column + index * sizeof(cType);
        return offset.as<MemRef>().load<typename ValueForPrimitive<cType>::type>();
    } else if constexpr (std::is_same_v<ArrowType, arrow::StringArray>) {
        return FunctionCall<>("getStringValue", getStringValue, column, index);
    } else {
        return FunctionCall<>("getValue" + std::string(typeid(ArrowType).name()), getArrayValue<ArrowType>, column, index);
    }
}

// Instantiate types
template class ArrowFieldReader<arrow::Int8Array>;
template class ArrowFieldReader<arrow::Int16Array>;
template class ArrowFieldReader<arrow::Int32Array>;
template class ArrowFieldReader<arrow::Int64Array>;
template class ArrowFieldReader<arrow::UInt8Array>;
template class ArrowFieldReader<arrow::UInt16Array>;
template class ArrowFieldReader<arrow::UInt32Array>;
template class ArrowFieldReader<arrow::UInt64Array>;
template class ArrowFieldReader<arrow::FloatArray>;
template class ArrowFieldReader<arrow::DoubleArray>;
template class ArrowFieldReader<arrow::BooleanArray>;
template class ArrowFieldReader<arrow::StringArray>;

std::vector<std::shared_ptr<AbstractArrowFieldReader>> createArrowFieldReaderFromSchema(const SchemaPtr& schema) {
    std::vector<std::shared_ptr<AbstractArrowFieldReader>> readers;
    auto df = DefaultPhysicalTypeFactory();
    for (auto fieldIndex = 0_u64; fieldIndex < schema->fields.size(); fieldIndex++) {
        auto& field = schema->fields[fieldIndex];
        auto dataType = field->getDataType();
        auto physicalType = df.getPhysicalType(dataType);
        if (physicalType->isBasicType()) {
            auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);

            switch (basicPhysicalType->nativeType) {
                case NES::BasicPhysicalType::NativeType::INT_8: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::Int8Array>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_16: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::Int16Array>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_32: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::Int32Array>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_64: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::Int64Array>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_8: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::UInt8Array>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_16: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::UInt16Array>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_32: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::UInt32Array>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_64: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::UInt64Array>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::FLOAT: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::FloatArray>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::DOUBLE: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::DoubleArray>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::TEXT: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::StringArray>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                case NES::BasicPhysicalType::NativeType::BOOLEAN: {
                    auto reader = std::make_shared<ArrowFieldReader<arrow::BooleanArray>>(fieldIndex, field->getName());
                    readers.emplace_back(reader);
                    break;
                }
                default: {
                    NES_NOT_IMPLEMENTED();
                }
            }
        } else {
            // We do not support any other ARROW types (such as Lists, Maps, Tensors) yet. We could however later store
            // them in the childBuffers similar to how we store TEXT and push the computation supported by arrow down to
            // them.
            NES_NOT_IMPLEMENTED();
        }
    }
    return readers;
}

}// namespace NES::Runtime::Execution::Operators
