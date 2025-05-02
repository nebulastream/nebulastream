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

#include <memory>
#include <string>
#include <utility>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <Common/DataTypes/Boolean.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>

namespace NES
{

SelectionLogicalOperator::SelectionLogicalOperator(LogicalFunction predicate) : predicate(predicate)
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

bool SelectionLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SelectionLogicalOperator*>(&rhs))
    {
        if (predicate != rhsOperator->predicate)
            return false;
        return true;
    }
    return false;
};

std::string SelectionLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    std::stringstream ss;
    if (verbosity == ExplainVerbosity::Debug)
    {
        ss << fmt::format("FILTER(opId: {}, predicate: {})", id, predicate.explain(verbosity));
    } else if (verbosity == ExplainVerbosity::Short)
    {
        ss << fmt::format("FILTER({})", predicate.explain(verbosity));
    }
    return ss.str();
}

LogicalOperator SelectionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(!inputSchemas.empty(), "Selection should have at least one input");
    
    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas) {
        if (schema != firstSchema) {
            throw CannotInferSchema("All input schemas must be equal for Selection operator");
        }
    }
    
    copy.predicate = predicate.withInferredStamp(firstSchema);
    if (*copy.predicate.getStamp().get() != Boolean())
    {
        throw CannotInferSchema("the selection expression is not a valid predicate");
    }
    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;
    return copy;
}

Optimizer::TraitSet SelectionLogicalOperator::getTraitSet() const
{
    return {};
}

LogicalOperator SelectionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
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

std::vector<std::vector<OriginId>> SelectionLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> SelectionLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator SelectionLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator SelectionLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> SelectionLogicalOperator::getChildren() const
{
    return children;
}

SerializableOperator SelectionLogicalOperator::serialize() const
{
    SerializableOperator_LogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (auto const& trait : getTraitSet()) {
        *traitSetProto->add_traits() = trait.serialize();
    }

    auto inputs     = getInputSchemas();
    auto originLists = getInputOriginIds();
    for (size_t i = 0; i < inputs.size(); ++i) {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], schProto);

        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originLists[i]) {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds()) {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (auto& child : getChildren()) {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    FunctionList funcList;
    auto* serializedFunction = funcList.add_functions();
    serializedFunction->CopyFrom(getPredicate().serialize());
    (*serializableOperator.mutable_config())[ConfigParameters::SELECTION_FUNCTION_NAME] =
    Configurations::descriptorConfigTypeToProto(funcList);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterSelectionLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto functionVariant = arguments.config[SelectionLogicalOperator::ConfigParameters::SELECTION_FUNCTION_NAME];
    if (std::holds_alternative<NES::FunctionList>(functionVariant))
    {
        const auto functions = std::get<FunctionList>(functionVariant).functions();

        INVARIANT(functions.size() == 1, "Expected exactly one function");
        auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);
        auto logicalOperator = SelectionLogicalOperator(function);
        if (auto& id = arguments.id) {
            logicalOperator.id = *id;
        }
        return logicalOperator.withInferredSchema(arguments.inputSchemas)
            .withInputOriginIds(arguments.inputOriginIds)
            .withOutputOriginIds(arguments.outputOriginIds);
    }
    throw UnknownLogicalOperator();
}
}
