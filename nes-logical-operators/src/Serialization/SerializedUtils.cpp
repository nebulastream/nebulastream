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
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Serialization/SerializedData.hpp>
#include <ErrorHandling.hpp>

#include "LogicalFunctionRegistry.hpp"

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


DataType deserializeDataType(const SerializedDataType& serializedDataType)
{
    switch (serializedDataType)
    {
        case SerializedDataType::UINT8:
            return DataType{DataType::Type::UINT8};
        case SerializedDataType::UINT16:
            return DataType{DataType::Type::UINT16};
        case SerializedDataType::UINT32:
            return DataType{DataType::Type::UINT32};
        case SerializedDataType::UINT64:
            return DataType{DataType::Type::UINT64};
        case SerializedDataType::BOOLEAN:
            return DataType{DataType::Type::BOOLEAN};
        case SerializedDataType::INT8:
            return DataType{DataType::Type::INT8};
        case SerializedDataType::INT16:
            return DataType{DataType::Type::INT16};
        case SerializedDataType::INT32:
            return DataType{DataType::Type::INT32};
        case SerializedDataType::INT64:
            return DataType{DataType::Type::INT64};
        case SerializedDataType::FLOAT32:
            return DataType{DataType::Type::FLOAT32};
        case SerializedDataType::FLOAT64:
            return DataType{DataType::Type::FLOAT64};
        case SerializedDataType::CHAR:
            return DataType{DataType::Type::CHAR};
        case SerializedDataType::UNDEFINED:
            return DataType{DataType::Type::UNDEFINED};
        case SerializedDataType::VARSIZED:
            return DataType{DataType::Type::VARSIZED};
        default:
            throw Exception{"Oh No", 9999}; // TODO Improve
    }
}

Schema::MemoryLayoutType deserializeMemoryLayout(const SerializedMemoryLayout& serializedMemoryLayout)
{
    switch (serializedMemoryLayout)
    {
        case SerializedMemoryLayout::ROW_LAYOUT:
            return Schema::MemoryLayoutType::ROW_LAYOUT;
        case SerializedMemoryLayout::COL_LAYOUT:
            return Schema::MemoryLayoutType::COLUMNAR_LAYOUT;
        default:
            throw Exception{"Oh No", 9999}; // TODO improve
    }
}

Schema deserializeSchema(const SerializedSchema& serialized)
{

    auto schema = Schema{deserializeMemoryLayout(serialized.memoryLayout)};
    for (auto &field: serialized.fields)
    {
        schema.addField(field.name, deserializeDataType(*field.type));
    }
    return schema;
}

std::vector<Schema> deserializeSchemas(const std::vector<SerializedSchema>& serializedSchemas)
{
    std::vector<Schema> schemas;
    for (const auto& schema : serializedSchemas)
    {
        schemas.emplace_back(deserializeSchema(schema));
    }
    return schemas;
}

LogicalFunction deserializeFunction(const SerializedFunction& serializedFunction)
{

    const auto& functionType = serializedFunction.functionType;

    std::vector<LogicalFunction> children;
    for (const auto& child : serializedFunction.children)
    {
        children.emplace_back(deserializeFunction(child));
    }

    auto dataType =  deserializeDataType(serializedFunction.dataType);


    // TODO this will be generalized. Just implemented this as a quick hack.
    if (functionType == "Equals")
    {
        return EqualsLogicalFunction{children[0], children[1]};
    } else if (functionType == "ConstantValue")
    {
        return ConstantValueLogicalFunction{dataType, serializedFunction.config.at("constantValueAsString").to_string().value()};

    } else if (functionType == "FieldAccess")
    {
        return FieldAccessLogicalFunction{dataType, serializedFunction.config.at("FieldName").to_string().value()};
    }
    throw Exception{"Oh No", 9999};
}
}