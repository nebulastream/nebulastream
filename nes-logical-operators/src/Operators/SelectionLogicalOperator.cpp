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

#include <Operators/SelectionLogicalOperator.hpp>

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

#include <rfl.hpp>
#include <rfl/json.hpp>

namespace NES
{

SelectionLogicalOperator::SelectionLogicalOperator(LogicalFunction predicate) : predicate(std::move(predicate))
{
}

std::string_view SelectionLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalFunction SelectionLogicalOperator::getPredicate() const
{
    return predicate;
}

bool SelectionLogicalOperator::operator==(const SelectionLogicalOperator& rhs) const
{
    return predicate == rhs.predicate && getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas()
        && getTraitSet() == rhs.getTraitSet();
};

std::string SelectionLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SELECTION(opId: {}, predicate: {}, traitSet: {})", opId, predicate.explain(verbosity), traitSet.explain(verbosity));
    }
    return fmt::format("SELECTION({})", predicate.explain(verbosity));
}

SelectionLogicalOperator SelectionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    if (inputSchemas.empty())
    {
        throw CannotDeserialize("Selection should have at least one input");
    }

    const auto& firstSchema = inputSchemas.at(0);
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Selection operator");
        }
    }

    copy.predicate = predicate.withInferredDataType(firstSchema);
    if (not copy.predicate.getDataType().isType(DataType::Type::BOOLEAN))
    {
        throw CannotInferSchema("the selection expression is not a valid predicate");
    }
    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;
    return copy;
}

TraitSet SelectionLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SelectionLogicalOperator SelectionLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

SelectionLogicalOperator SelectionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> SelectionLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema SelectionLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> SelectionLogicalOperator::getChildren() const
{
    return children;
}


enum class DataTypeSerializer: int8_t
{
    UINT8, UINT16, UINT32, UINT64, INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, CHAR, UNDEFINED, VARSIZED
};

struct FieldSerializer
{
    std::string name;
    rfl::Box<DataTypeSerializer> type;
};

enum class MemoryLayoutSerializer: int8_t
{
    ROW_LAYOUT, COL_LAYOUT
};

struct SchemaSerializer
{
    MemoryLayoutSerializer memoryLayout;
    std::vector<FieldSerializer> fields;
};

// std::variant<bool, int, double, std::string, rfl::Object, rfl::Array, std::nullopt_t>;

struct FunctionSerializer
{
    std::vector<FunctionSerializer> children;
    std::string functionType;
    DataTypeSerializer dataType;
    std::map<std::string, rfl::Generic> config;
};

struct SelectionLogicalOperatorSerializer
{
    std::vector<SchemaSerializer> inputSchemas;
    rfl::Box<SchemaSerializer> outputSchema;
    std::vector<uint64_t> childrenIds;
    rfl::Box<FunctionSerializer> predicate;
};


DataTypeSerializer serialize(DataType dataType)
{
    switch (dataType.type)
    {
        case DataType::Type::UINT8: return DataTypeSerializer::UINT8;
        case DataType::Type::UINT16: return DataTypeSerializer::UINT16;
        case DataType::Type::UINT32: return DataTypeSerializer::UINT32;
        case DataType::Type::UINT64: return DataTypeSerializer::UINT64;
        case DataType::Type::BOOLEAN: return DataTypeSerializer::BOOLEAN;
        case DataType::Type::INT8: return DataTypeSerializer::INT8;
        case DataType::Type::INT16: return DataTypeSerializer::INT16;
        case DataType::Type::INT32: return DataTypeSerializer::INT32;
        case DataType::Type::INT64: return DataTypeSerializer::INT64;
        case DataType::Type::FLOAT32: return DataTypeSerializer::FLOAT32;
        case DataType::Type::FLOAT64: return DataTypeSerializer::FLOAT64;
        case DataType::Type::CHAR: return DataTypeSerializer::CHAR;
        case DataType::Type::UNDEFINED: return DataTypeSerializer::UNDEFINED;
        case DataType::Type::VARSIZED: return DataTypeSerializer::VARSIZED;
        default: throw Exception{"Oh No", 9999}; // TODO Improve
    }
}


SchemaSerializer serialize_schema(Schema schema)
{
    SchemaSerializer serializer;

    serializer.memoryLayout =
        schema.memoryLayoutType == Schema::MemoryLayoutType::ROW_LAYOUT ?
            MemoryLayoutSerializer::ROW_LAYOUT:
            MemoryLayoutSerializer::COL_LAYOUT;
    for (const Schema::Field& field : schema.getFields())
    {
        serializer.fields.emplace_back(field.name, rfl::make_box<DataTypeSerializer>(serialize(field.dataType)));
    }
    return serializer;
}



void SelectionLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    auto data = SelectionLogicalOperatorSerializer{};

    for (const auto& inputSchema: getInputSchemas())
    {
        data.inputSchemas.emplace_back(serialize_schema(inputSchema));
    }

    data.outputSchema = rfl::make_box<SchemaSerializer>(serialize_schema(outputSchema));

    for (const auto& child: children)
    {
        data.childrenIds.emplace_back(child.getId().getRawValue());
    }


    // TEMP
    FunctionSerializer function;
    auto value = rfl::to_generic(FieldSerializer{.name="main", .type=rfl::make_box<DataTypeSerializer>(DataTypeSerializer::VARSIZED)});
    function.config.emplace("test", value);


    data.predicate = rfl::make_box<FunctionSerializer>(function);


    const auto serializedString = rfl::json::write(data);


    serializableOperator.set_reflect(serializedString);

    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    for (const auto& input : getInputSchemas())
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(input, schProto);
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    FunctionList funcList;
    auto* serializedFunction = funcList.add_functions();
    serializedFunction->CopyFrom(getPredicate().serialize());
    (*serializableOperator.mutable_config())[ConfigParameters::SELECTION_FUNCTION_NAME] = descriptorConfigTypeToProto(funcList);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterSelectionLogicalOperator(LogicalOperatorRegistryArguments arguments)
{

    auto data = rfl::json::read<SelectionLogicalOperatorSerializer>(arguments.reflec).value();


    auto functionVariant = arguments.config.at(SelectionLogicalOperator::ConfigParameters::SELECTION_FUNCTION_NAME);
    if (std::holds_alternative<FunctionList>(functionVariant))
    {
        const auto functions = std::get<FunctionList>(functionVariant).functions();

        if (functions.size() != 1)
        {
            throw CannotDeserialize("Expected exactly one function but got {}", functions.size());
        }
        auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);
        auto logicalOperator = SelectionLogicalOperator(function);
        return logicalOperator.withInferredSchema(arguments.inputSchemas);
    }
    throw UnknownLogicalOperator();
}
}
