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

#include <algorithm>
#include <memory>
#include <string>
#include <Configurations/Descriptor.hpp>
#include <LogicalOperators/Operator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FunctionRegistry.hpp>
#include <OperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableSchema.pb.h>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <fmt/format.h>
#include <LogicalOperators/MapOperator.hpp>

namespace NES::Logical
{

MapOperator::MapOperator(const FieldAssignmentFunction& mapFunction) : mapFunction(mapFunction)
{
}

std::string_view MapOperator::getName() const noexcept
{
    return NAME;
}

const FieldAssignmentFunction& MapOperator::getMapFunction() const
{
    return mapFunction;
}

bool MapOperator::operator==(const OperatorConcept& rhs) const
{
    if (auto other = dynamic_cast<const MapOperator*>(&rhs))
    {
        return this->mapFunction == other->mapFunction &&
               getOutputSchema() == other->getOutputSchema() &&
               getInputSchemas() == other->getInputSchemas() &&
               getInputOriginIds() == other->getInputOriginIds() &&
               getOutputOriginIds() == other->getOutputOriginIds() &&
               getChildren() == other->getChildren();
    }
    return false;
};

Operator MapOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    INVARIANT(inputSchemas.size() == 1, "Map should have one input but got {}", inputSchemas.size());
    
    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas) {
        if (schema != firstSchema) {
            throw CannotInferSchema("All input schemas must be equal for Map operator");
        }
    }

    auto copy = *this;
    /// use the default input schema to calculate the output schema of this operator.
    copy.mapFunction = mapFunction.withInferredStamp(firstSchema).get<FieldAssignmentFunction>();

    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;

    if (std::string fieldName = copy.mapFunction.getField().getFieldName(); copy.outputSchema.getFieldByName(fieldName))
    {
        /// The assigned field is part of the current schema.
        /// Thus we check if it has the correct type.
        NES_TRACE("The field {} is already in the schema, so we updated its type.", fieldName);
        copy.outputSchema.replaceField(fieldName, copy.mapFunction.getField().getStamp());
    }
    else
    {
        /// The assigned field is not part of the current schema.
        /// Thus we extend the schema by the new attribute.
        NES_TRACE("The field {} is not part of the schema, so we added it.", fieldName);
        copy.outputSchema.addField(fieldName, copy.mapFunction.getField().getStamp());
    }

    return copy;
}

Optimizer::TraitSet MapOperator::getTraitSet() const
{
    return {};
}

Operator MapOperator::withChildren(std::vector<Operator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> MapOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema MapOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> MapOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> MapOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

Operator MapOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Selection should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

Operator MapOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<Operator> MapOperator::getChildren() const
{
    return children;
}

std::string MapOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("MAP(opId: {}, function: {})", id, mapFunction.explain(verbosity));
    }
    return fmt::format("MAP({})", mapFunction.explain(verbosity));
}

SerializableOperator MapOperator::serialize() const
{
    SerializableOperator_LogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (auto const& trait : getTraitSet()) {
        *traitSetProto->add_traits() = trait.serialize();
    }

    const auto inputs      = getInputSchemas();
    const auto originLists = getInputOriginIds();
    for (size_t i = 0; i < inputs.size(); ++i) {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], inSch);

        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originLists[i]) {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds()) {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (auto& child : getChildren()) {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    FunctionList funcList;
    *funcList.add_functions() = getMapFunction().serialize();
    (*serializableOperator.mutable_config())["mapFunction"] =
        Configurations::descriptorConfigTypeToProto(funcList);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

NES::Configurations::DescriptorConfig::Config MapOperator::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return NES::Configurations::DescriptorConfig::validateAndFormat<ConfigParameters>(std::move(config), NAME);
}

OperatorRegistryReturnType
OperatorGeneratedRegistrar::RegisterMapOperator(OperatorRegistryArguments arguments)
{
    auto functionVariant = arguments.config[MapOperator::ConfigParameters::MAP_FUNCTION_NAME];
    if (std::holds_alternative<NES::FunctionList>(functionVariant))
    {
        const auto functions = std::get<FunctionList>(functionVariant).functions();

        INVARIANT(functions.size() == 1, "Expected exactly one function");
        auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);
        INVARIANT(function.tryGet<FieldAssignmentFunction>(), "Expected a field assignment function, got: {}", function.explain(ExplainVerbosity::Debug));

        auto logicalOperator = MapOperator(function.get<FieldAssignmentFunction>());
        if (auto& id = arguments.id) {
            logicalOperator.id = *id;
        }
        return logicalOperator.withInferredSchema(arguments.inputSchemas)
            .withInputOriginIds(arguments.inputOriginIds)
            .withOutputOriginIds(arguments.outputOriginIds);    }
    throw UnknownOperator();
}

}
