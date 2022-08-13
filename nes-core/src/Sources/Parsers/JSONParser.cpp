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

#include "API/AttributeField.hpp"
#include "Common/DataTypes/DataType.hpp"
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <simdjson.h>
#include <string>
#include <utility>

namespace NES {

JSONParser::JSONParser(std::vector<PhysicalTypePtr> physicalTypes) : physicalTypes(std::move(physicalTypes)){};

void JSONParser::writeFieldValueToTupleBuffer(
    SchemaPtr& schema,// TODO don't have to pass schema every time → global class variable
    uint64_t tupleIndex,
    simdjson::simdjson_result<simdjson::dom::element> element,
    Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer) {
    for (uint64_t fieldIndex = 0; fieldIndex < schema->getSize(); fieldIndex++) {
        std::string jsonKey = schema->fields[fieldIndex]->getName();
        std::shared_ptr<PhysicalType> physicalType = physicalTypes[fieldIndex];
        if (physicalType->isBasicType()) {
            BasicPhysicalTypePtr basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
            BasicPhysicalType::NativeType nativeType = basicPhysicalType->nativeType;
            switch (nativeType) {
                case BasicPhysicalType::UINT_8: {
                    uint8_t value8 = static_cast<uint8_t>(element[jsonKey].get_uint64());
                    tupleBuffer[tupleIndex][fieldIndex].write<uint8_t>(value8);
                    break;
                }
                case BasicPhysicalType::UINT_16: {
                    uint16_t value16 = static_cast<uint16_t>(element[jsonKey].get_uint64());
                    tupleBuffer[tupleIndex][fieldIndex].write<uint16_t>(value16);
                    break;
                }
                case BasicPhysicalType::UINT_32: {
                    uint32_t value32 = static_cast<uint32_t>(element[jsonKey].get_uint64());
                    tupleBuffer[tupleIndex][fieldIndex].write<uint32_t>(value32);
                    break;
                }
                case BasicPhysicalType::UINT_64: {
                    uint64_t value = element[jsonKey];
                    tupleBuffer[tupleIndex][fieldIndex].write<uint64_t>(value);
                    break;
                }
                case BasicPhysicalType::INT_8: {
                    int8_t value8 = static_cast<int8_t>(element[jsonKey].get_int64());
                    tupleBuffer[tupleIndex][fieldIndex].write<int8_t>(value8);
                    break;
                }
                case BasicPhysicalType::INT_16: {
                    int16_t value16 = static_cast<int16_t>(element[jsonKey].get_int64());
                    tupleBuffer[tupleIndex][fieldIndex].write<int16_t>(value16);
                    break;
                }
                case BasicPhysicalType::INT_32: {
                    int32_t value32 = static_cast<int32_t>(element[jsonKey].get_int64());
                    tupleBuffer[tupleIndex][fieldIndex].write<int32_t>(value32);
                    break;
                }
                case BasicPhysicalType::INT_64: {
                    int64_t value = element[jsonKey];
                    tupleBuffer[tupleIndex][fieldIndex].write<int64_t>(value);
                    break;
                }
                case BasicPhysicalType::FLOAT: {
                    float valueF = static_cast<float>(element[jsonKey].get_double());
                    tupleBuffer[tupleIndex][fieldIndex].write<float>(valueF);
                    break;
                }
                case BasicPhysicalType::DOUBLE: {// TODO is Float
                    /*
                    auto value = element[jsonKey]; // ← correct value
                    std::cout << "value: " << value << std::endl;
                    double d = value.get_double(); // ← cropped
                    std::cout << "value d: " << d << std::endl;
                    */

                    double value = element[jsonKey];
                    tupleBuffer[tupleIndex][fieldIndex].write<double>(value);
                    break;
                }
                case BasicPhysicalType::CHAR: {
                    const char* c = element[jsonKey].get_c_str();
                    tupleBuffer[tupleIndex][fieldIndex].write<char>(c[0]);
                    break;
                }
                case BasicPhysicalType::BOOLEAN: {
                    bool value = element[jsonKey];
                    tupleBuffer[tupleIndex][fieldIndex].write<bool>(value);
                    break;
                }
            }
        } else {
            DataTypePtr dataType = schema->fields[fieldIndex]->getDataType();
            if (dataType->isCharArray()) {
                std::string_view value = element[jsonKey];
                std::string str = {value.begin(), value.end()};
                const char* c_str = str.c_str();
                char* valueRead = tupleBuffer[tupleIndex][fieldIndex].read<char*>();
                strcpy(valueRead, c_str);
            } else if (dataType->isUndefined()) {
                NES_ERROR("Invalid data type")
                throw std::invalid_argument("Data type is undefined");
            } else {
                NES_ERROR("Invalid DataType: " << dataType);
                throw std::invalid_argument("Invalid DataType: " + dataType->toString());
            }
        }
    }
}

}// namespace NES