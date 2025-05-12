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

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/MapLogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

MapLogicalOperator::MapLogicalOperator(FieldAssignmentLogicalFunction mapFunction) : mapFunction(std::move(mapFunction))
{
}

std::string_view MapLogicalOperator::getName() const noexcept
{
    return NAME;
}

const FieldAssignmentLogicalFunction& MapLogicalOperator::getMapFunction() const
{
    return mapFunction;
}

bool MapLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const MapLogicalOperator*>(&rhs))
    {
        return this->mapFunction == other->mapFunction && getOutputSchema() == other->getOutputSchema()
            && getInputSchemas() == other->getInputSchemas() && getInputOriginIds() == other->getInputOriginIds()
            && getOutputOriginIds() == other->getOutputOriginIds() && getChildren() == other->getChildren()
            && getTraitSet() == other->getTraitSet();
    }
    return false;
}

LogicalOperator MapLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    INVARIANT(inputSchemas.size() == 1, "Map should have one input but got {}", inputSchemas.size());

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Map operator");
        }
    }

    auto copy = *this;
    /// use the default input schema to calculate the output schema of this operator.
    copy.mapFunction = mapFunction.withInferredDataType(firstSchema).get<FieldAssignmentLogicalFunction>();

    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;

    if (std::string fieldName = copy.mapFunction.getField().getFieldName(); copy.outputSchema.getFieldByName(fieldName))
    {
        /// The assigned field is part of the current schema.
        /// Thus we check if it has the correct type.
        NES_TRACE("The field {} is already in the schema, so we updated its type.", fieldName);
        if (not copy.outputSchema.replaceTypeOfField(fieldName, copy.mapFunction.getField().getDataType()))
        {
            throw CannotInferSchema("Could not replace field {} with {}", fieldName, copy.mapFunction.getField().getDataType());
        }
    }
    else
    {
        /// The assigned field is not part of the current schema.
        /// Thus we extend the schema by the new attribute.
        NES_TRACE("The field {} is not part of the schema, so we added it.", fieldName);
        copy.outputSchema.addField(fieldName, copy.mapFunction.getField().getDataType());
    }

    return copy;
}

TraitSet MapLogicalOperator::getTraitSet() const
{
    return traitSet;
}

LogicalOperator MapLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

LogicalOperator MapLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

std::vector<Schema> MapLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema MapLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> MapLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> MapLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator MapLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Selection should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator MapLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> MapLogicalOperator::getChildren() const
{
    return children;
}

std::string MapLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("MAP(opId: {}, function: {})", id, mapFunction.explain(verbosity));
    }
    return fmt::format("MAP({})", mapFunction.explain(verbosity));
}

SerializableOperator MapLogicalOperator::serialize() const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (const auto& trait : getTraitSet())
    {
        *traitSetProto->add_traits() = trait.serialize();
    }

    const auto inputs = getInputSchemas();
    const auto originLists = getInputOriginIds();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], inSch);

        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originLists[i])
        {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds())
    {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    FunctionList funcList;
    *funcList.add_functions() = getMapFunction().serialize();
    (*serializableOperator.mutable_config())["mapFunction"] = Configurations::descriptorConfigTypeToProto(funcList);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

NES::Configurations::DescriptorConfig::Config MapLogicalOperator::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return NES::Configurations::DescriptorConfig::validateAndFormat<ConfigParameters>(std::move(config), NAME);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterMapLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto functionVariant = arguments.config[MapLogicalOperator::ConfigParameters::MAP_FUNCTION_NAME];
    if (std::holds_alternative<NES::FunctionList>(functionVariant))
    {
        const auto functions = std::get<FunctionList>(functionVariant).functions();

        INVARIANT(functions.size() == 1, "Expected exactly one function");
        auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);
        INVARIANT(
            function.tryGet<FieldAssignmentLogicalFunction>(),
            "Expected a field assignment function, got: {}",
            function.explain(ExplainVerbosity::Debug));

        auto logicalOperator = MapLogicalOperator(function.get<FieldAssignmentLogicalFunction>());
        if (auto& id = arguments.id)
        {
            logicalOperator.id = *id;
        }
        return logicalOperator.withInferredSchema(arguments.inputSchemas)
            .withInputOriginIds(arguments.inputOriginIds)
            .withOutputOriginIds(arguments.outputOriginIds);
    }
    throw UnknownLogicalOperator();
}

}
