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
#include <Serialization/SerializedUtils.hpp>

#include <Serialization/SerializedData.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>


namespace NES
{
SerializedDataType serializeDataType(const DataType& dataType)
{
    switch (dataType.type)
    {
        case DataType::Type::UINT8:
            return SerializedDataType::UINT8;
        case DataType::Type::UINT16:
            return SerializedDataType::UINT16;
        case DataType::Type::UINT32:
            return SerializedDataType::UINT32;
        case DataType::Type::UINT64:
            return SerializedDataType::UINT64;
        case DataType::Type::BOOLEAN:
            return SerializedDataType::BOOLEAN;
        case DataType::Type::INT8:
            return SerializedDataType::INT8;
        case DataType::Type::INT16:
            return SerializedDataType::INT16;
        case DataType::Type::INT32:
            return SerializedDataType::INT32;
        case DataType::Type::INT64:
            return SerializedDataType::INT64;
        case DataType::Type::FLOAT32:
            return SerializedDataType::FLOAT32;
        case DataType::Type::FLOAT64:
            return SerializedDataType::FLOAT64;
        case DataType::Type::CHAR:
            return SerializedDataType::CHAR;
        case DataType::Type::UNDEFINED:
            return SerializedDataType::UNDEFINED;
        case DataType::Type::VARSIZED:
            return SerializedDataType::VARSIZED;
        default:
            throw Exception{"Oh No", 9999}; // TODO Improve
    }
}

SerializedSchema serializeSchema(const Schema& schema)
{
    SerializedSchema serializer;

    serializer.memoryLayout =
        schema.memoryLayoutType == Schema::MemoryLayoutType::ROW_LAYOUT ?
            SerializedMemoryLayout::ROW_LAYOUT:
            SerializedMemoryLayout::COL_LAYOUT;
    for (const Schema::Field& field : schema.getFields())
    {
        serializer.fields.emplace_back(field.name, rfl::make_box<SerializedDataType>(serializeDataType(field.dataType)));
    }
    return serializer;
}
}