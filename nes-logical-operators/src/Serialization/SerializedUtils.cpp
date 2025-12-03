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

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SerializedData.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
SerializedDataType SerializedUtils::serializeDataType(const DataType& dataType)
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
            throw Exception{"Oh No", 9999}; /// TODO Improve error handling
    }
}

SerializedSchema SerializedUtils::serializeSchema(const Schema& schema)
{
    SerializedSchema serializer;

    serializer.memoryLayout = schema.memoryLayoutType == Schema::MemoryLayoutType::ROW_LAYOUT ? SerializedMemoryLayout::ROW_LAYOUT
                                                                                              : SerializedMemoryLayout::COL_LAYOUT;
    for (const Schema::Field& field : schema.getFields())
    {
        serializer.fields.emplace_back(field.name, rfl::make_box<SerializedDataType>(serializeDataType(field.dataType)));
    }
    return serializer;
}

DataType SerializedUtils::deserializeDataType(const SerializedDataType& serializedDataType)
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
            throw Exception{"Oh No", 9999}; /// TODO: Improve error handling
    }
}

Schema::MemoryLayoutType SerializedUtils::deserializeMemoryLayout(const SerializedMemoryLayout& serializedMemoryLayout)
{
    switch (serializedMemoryLayout)
    {
        case SerializedMemoryLayout::ROW_LAYOUT:
            return Schema::MemoryLayoutType::ROW_LAYOUT;
        case SerializedMemoryLayout::COL_LAYOUT:
            return Schema::MemoryLayoutType::COLUMNAR_LAYOUT;
        default:
            throw Exception{"Oh No", 9999}; /// TODO: Improve error handling
    }
}

Schema SerializedUtils::deserializeSchema(const SerializedSchema& serialized)
{
    auto schema = Schema{deserializeMemoryLayout(serialized.memoryLayout)};
    for (auto& field : serialized.fields)
    {
        schema.addField(field.name, deserializeDataType(*field.type));
    }
    return schema;
}

std::vector<Schema> SerializedUtils::deserializeSchemas(const std::vector<SerializedSchema>& serializedSchemas)
{
    std::vector<Schema> schemas;
    for (const auto& schema : serializedSchemas)
    {
        schemas.emplace_back(deserializeSchema(schema));
    }
    return schemas;
}

LogicalFunction SerializedUtils::deserializeFunction(const SerializedFunction& serializedFunction)
{
    const auto& functionType = serializedFunction.functionType;

    std::vector<LogicalFunction> children;
    for (const auto& child : serializedFunction.children)
    {
        children.emplace_back(deserializeFunction(child));
    }

    auto dataType = deserializeDataType(serializedFunction.dataType);


    /// TODO: this will be generalized. Just implemented this as a quick hack.
    if (functionType == "Equals")
    {
        return EqualsLogicalFunction{children[0], children[1]};
    }
    else if (functionType == "ConstantValue")
    {
        return ConstantValueLogicalFunction{dataType, serializedFunction.config.at("constantValueAsString").to_string().value()};
    }
    else if (functionType == "FieldAccess")
    {
        return FieldAccessLogicalFunction{dataType, serializedFunction.config.at("FieldName").to_string().value()};
    }
    throw Exception{"Oh No", 9999}; /// TODO: Improve Error handling
}

/// TODO: Everything related to Source Descriptor can be simplified once DescriptorConfig::ConfigType stops relying on Protobuf classes
///       will be updated in a separate PR.
ConfigValue SerializedUtils::serializeDescriptorConfigValue(DescriptorConfig::ConfigType value)
{
    ConfigValue serialized;
    std::visit(
        [&serialized]<typename T>(T&& arg)
        {
            using U = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<U, int32_t>)
            {
                serialized.type = "int32_t";
                serialized.int32 = arg;
            }
            else if constexpr (std::is_same_v<U, uint32_t>)
            {
                serialized.type = "uint32_t";
                serialized.uint32 = arg;
            }
            else if constexpr (std::is_same_v<U, int64_t>)
            {
                serialized.type = "int64_t";
                serialized.int64 = arg;
            }
            else if constexpr (std::is_same_v<U, uint64_t>)
            {
                serialized.type = "uint64_t";
                serialized.uint64 = arg;
            }
            else if constexpr (std::is_same_v<U, bool>)
            {
                serialized.type = "bool";
                serialized.boolean = arg;
            }
            else if constexpr (std::is_same_v<U, char>)
            {
                serialized.type = "char";
                serialized.character = arg;
            }
            else if constexpr (std::is_same_v<U, float>)
            {
                serialized.type = "float";
                serialized.float_v = arg;
            }
            else if constexpr (std::is_same_v<U, double>)
            {
                serialized.type = "double";
                serialized.double_v = arg;
            }
            else if constexpr (std::is_same_v<U, std::string>)
            {
                serialized.type = "string";
                serialized.string = arg;
            }
            else if constexpr (std::is_same_v<U, EnumWrapper>)
            {
                serialized.type = "Enum";
                serialized.string = arg.getValue();
            }
            else if constexpr (std::is_same_v<U, FunctionList>)
            {
                std::vector<SerializedFunction> serializedFunctions;
                for (auto function : arg.functions())
                {
                    LogicalFunction logicalFunction = FunctionSerializationUtil::deserializeFunction(function);
                    ;
                    if (logicalFunction.getType() == "Equals")
                    {
                        serializedFunctions.emplace_back(logicalFunction.get<EqualsLogicalFunction>().serialized());
                    }
                    else if (logicalFunction.getType() == "ConstantValue")
                    {
                        serializedFunctions.emplace_back(logicalFunction.get<ConstantValueLogicalFunction>().serialized());
                    }
                    else if (logicalFunction.getType() == "FieldAccess")
                    {
                        serializedFunctions.emplace_back(logicalFunction.get<FieldAccessLogicalFunction>().serialized());
                    }
                }
                serialized.type = "functionList";
                serialized.generic = rfl::to_generic(serializedFunctions);
            }
            else if constexpr (std::is_same_v<U, ProjectionList>)
            {
                throw NotImplemented();
            }
            else if constexpr (std::is_same_v<U, AggregationFunctionList>)
            {
                throw NotImplemented();
            }
            else if constexpr (std::is_same_v<U, WindowInfos>)
            {
                throw NotImplemented();
            }
            else if constexpr (std::is_same_v<U, UInt64List>)
            {
                throw NotImplemented();
            }
            else
            {
                static_assert(!std::is_same_v<U, U>, "Unsupported type in SourceDescriptorConfigTypeToProto"); /// is_same_v for logging T
            }
        },
        value);

    return serialized;
}

SerializedDescriptorConfig SerializedUtils::serializeDescriptorConfig(const DescriptorConfig::Config& config)
{
    std::map<std::string, ConfigValue> serializedConfig;
    for (const auto& [key, value] : config)
    {
        serializedConfig.emplace(key, serializeDescriptorConfigValue(value));
    }

    return SerializedDescriptorConfig{serializedConfig};
}

DescriptorConfig::ConfigType SerializedUtils::deserializeDescriptorConfigValue(ConfigValue configValue)
{
    if (configValue.type == "int32_t")
    {
        return configValue.int32;
    }
    if (configValue.type == "uint32_t")
    {
        return configValue.uint32;
    }
    if (configValue.type == "int64_t")
    {
        return configValue.int64;
    }
    if (configValue.type == "uint64_t")
    {
        return configValue.uint64;
    }
    if (configValue.type == "bool")
    {
        return configValue.boolean;
    }
    if (configValue.type == "char")
    {
        return configValue.character;
    }
    if (configValue.type == "float")
    {
        return configValue.float_v;
    }
    if (configValue.type == "double")
    {
        return configValue.double_v;
    }
    if (configValue.type == "string")
    {
        return configValue.string;
    }
    if (configValue.type == "Enum")
    {
        return EnumWrapper{configValue.string};
    }
    if (configValue.type == "functionList")
    {
        auto serializedFunctionsOpt = rfl::from_generic<std::vector<SerializedFunction>>(configValue.generic);
        if (!serializedFunctionsOpt.has_value())
        {
            throw NotImplemented(); /// TODO: Improve error handling
        }
        auto serializedFunctions = serializedFunctionsOpt.value();
        FunctionList functionList;
        for (const auto& serialized : serializedFunctions)
        {
            auto function = deserializeFunction(serialized);
            auto* serializedFunction = functionList.add_functions();
            serializedFunction->CopyFrom(function.serialize());
        }
        return functionList;
    }
    throw NotImplemented();
}

DescriptorConfig::Config SerializedUtils::deserializeDescriptorConfig(SerializedDescriptorConfig serializedConfig)
{
    DescriptorConfig::Config config;

    for (const auto& [key, value] : serializedConfig.config)
    {
        config.emplace(key, deserializeDescriptorConfigValue(value));
    }

    return config;
}

SourceDescriptor SerializedUtils::deserializeSourceDescriptor(const SerializedSourceDescriptor& serialized)
{
    auto schema = deserializeSchema(serialized.schema);
    LogicalSource logicalSource{serialized.name, schema};

    const auto physicalSourceId = PhysicalSourceId{serialized.physicalSourceId};
    const auto& sourceType = serialized.type;
    ParserConfig parserConfig;
    parserConfig.parserType = serialized.parserConfig->parserType;
    parserConfig.tupleDelimiter = serialized.parserConfig->tupleDelimiter;
    parserConfig.fieldDelimiter = serialized.parserConfig->fieldDelimiter;

    auto serializedDescriptorConfig = rfl::from_generic<SerializedDescriptorConfig>(serialized.config).value();
    DescriptorConfig::Config config = deserializeDescriptorConfig(serializedDescriptorConfig);

    return SourceDescriptor{physicalSourceId, logicalSource, sourceType, std::move(config), parserConfig};
}
}
