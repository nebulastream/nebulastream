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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Sinks/Formats/FormatIterators/Iterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <nlohmann/json.hpp>
namespace NES {

std::string Iterator::serializeTupleAsJson() {
    uint8_t* tuplePointer = &this->buffer.getBuffer<uint8_t>()[currentSeek];

    // Iterate over all fields in a tuple.
    // Use the field name in the schema as the name in the JSON object
    // Use the offset array to determine where in the buffer the data for a field is stored.
    auto jsonObject = nlohmann::json{};
    for (uint32_t fieldIndex = 0; fieldIndex < fieldNames.size(); fieldIndex++) {
        auto physicalType = fieldTypes[fieldIndex];
        auto fieldName = fieldNames[fieldIndex];
        auto fieldData = tuplePointer + fieldOffsets[fieldIndex];
        if (physicalType->type->isText()) {
            auto currentFieldType = fieldTypes[fieldIndex];
            auto dataTypePtr = this->buffer.loadChildBuffer(*reinterpret_cast<uint32_t*>(fieldData)).getBuffer();
            auto fieldValue = currentFieldType->convertRawToStringWithoutFill(dataTypePtr);
            jsonObject[fieldName] = fieldValue;
        } else if (physicalType->isBasicType()) {
            auto basicFieldType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
            // Just reinterpret the binary data at the field-specific offset in the tuple buffer.
            switch (basicFieldType->nativeType) {
                using enum NES::BasicPhysicalType::NativeType;
                case INT_8: jsonObject[fieldName] = *reinterpret_cast<int8_t const*>(fieldData); break;
                case INT_16: jsonObject[fieldName] = *reinterpret_cast<int16_t const*>(fieldData); break;
                case INT_32: jsonObject[fieldName] = *reinterpret_cast<int32_t const*>(fieldData); break;
                case INT_64: jsonObject[fieldName] = *reinterpret_cast<int64_t const*>(fieldData); break;
                case UINT_8: jsonObject[fieldName] = *reinterpret_cast<uint8_t const*>(fieldData); break;
                case UINT_16: jsonObject[fieldName] = *reinterpret_cast<uint16_t const*>(fieldData); break;
                case UINT_32: jsonObject[fieldName] = *reinterpret_cast<uint32_t const*>(fieldData); break;
                case UINT_64: jsonObject[fieldName] = *reinterpret_cast<uint64_t const*>(fieldData); break;
                case FLOAT: jsonObject[fieldName] = *reinterpret_cast<float const*>(fieldData); break;
                case DOUBLE: jsonObject[fieldName] = *reinterpret_cast<double const*>(fieldData); break;
                case BOOLEAN: jsonObject[fieldName] = *reinterpret_cast<bool const*>(fieldData); break;
                case CHAR:
                    jsonObject[fieldName] = *reinterpret_cast<char const*>(fieldData);
                    break;// TODO: Remove support for single CHAR fields: https://github.com/nebulastream/nebulastream/issues/4941
                case UNDEFINED:
                    NES_FATAL_ERROR("Encountered unsupported type during conversion to JSON: {}", physicalType->toString());
            }
        } else {
            // TODO Remove Support for CHAR arrays: https://github.com/nebulastream/nebulastream/issues/4907
            NES_FATAL_ERROR("Encountered unsupported type during conversion to JSON: {}", physicalType->toString());
        }
    }
    return jsonObject.dump();
}

}// namespace NES
