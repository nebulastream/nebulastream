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
#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <LogicalOperators/Operator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <ErrorHandling.hpp>
#include <OperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <Common/DataTypes/Boolean.hpp>
#include <LogicalOperators/SelectionOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>

namespace NES::Logical
{

SelectionOperator::SelectionOperator(Function predicate) : predicate(predicate)
{
}

std::string_view SelectionOperator::getName() const noexcept
{
    return NAME;
}

Function SelectionOperator::getPredicate() const
{
    return predicate;
}

bool SelectionOperator::operator==(const OperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SelectionOperator*>(&rhs))
    {
        return predicate == rhsOperator->predicate &&
               getOutputSchema() == rhsOperator->getOutputSchema() &&
               getInputSchemas() == rhsOperator->getInputSchemas() &&
               getInputOriginIds() == rhsOperator->getInputOriginIds() &&
               getOutputOriginIds() == rhsOperator->getOutputOriginIds();
    }
    return false;
};

std::string SelectionOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SELECTION(opId: {}, predicate: {})", id, predicate.explain(verbosity));
    }
    return fmt::format("SELECTION({})", predicate.explain(verbosity));
}

Operator SelectionOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
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

Optimizer::TraitSet SelectionOperator::getTraitSet() const
{
    return {};
}

Operator SelectionOperator::withChildren(std::vector<Operator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SelectionOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema SelectionOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> SelectionOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> SelectionOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

Operator SelectionOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

Operator SelectionOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<Operator> SelectionOperator::getChildren() const
{
    return children;
}

SerializableOperator SelectionOperator::serialize() const
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

OperatorRegistryReturnType
OperatorGeneratedRegistrar::RegisterSelectionOperator(OperatorRegistryArguments arguments)
{
    auto functionVariant = arguments.config[SelectionOperator::ConfigParameters::SELECTION_FUNCTION_NAME];
    if (std::holds_alternative<NES::FunctionList>(functionVariant))
    {
        const auto functions = std::get<FunctionList>(functionVariant).functions();

        INVARIANT(functions.size() == 1, "Expected exactly one function");
        auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);
        auto logicalOperator = SelectionOperator(function);
        if (auto& id = arguments.id) {
            logicalOperator.id = *id;
        }
        return logicalOperator.withInferredSchema(arguments.inputSchemas)
            .withInputOriginIds(arguments.inputOriginIds)
            .withOutputOriginIds(arguments.outputOriginIds);
    }
    throw UnknownOperator();
}
}
