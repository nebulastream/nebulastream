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

#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Serialization/SerializedData.hpp>
#include <Serialization/SerializedUtils.hpp>
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

#include <rfl/json.hpp>
#include <rfl.hpp>

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

struct SerializedSelectionLogicalOperator
{
    rfl::Box<SerializedFunction> predicate;
};

void SelectionLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    for (const auto& input : getInputSchemas())
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(input, schProto);
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

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

void SelectionLogicalOperator::serialized(SerializedOperator& serialized) const
{
    /// TODO: If all functions support the serialized function, the if-clause and tryGet call can be dropped
    if (getPredicate().getType() == "Equals")
    {
        auto predicate = getPredicate().tryGet<EqualsLogicalFunction>()->serialized();
        const auto config = SerializedSelectionLogicalOperator{.predicate = rfl::make_box<SerializedFunction>(predicate)};
        serialized.config = rfl::to_generic(config);
    }
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterSelectionLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    const auto data = rfl::json::read<SerializedOperator>(arguments.reflec).value();
    if (auto serializedOperator = rfl::from_generic<SerializedSelectionLogicalOperator>(data.config); serializedOperator.has_value())
    {
        const auto predicate = SerializedUtils::deserializeFunction(*serializedOperator.value().predicate);
        const auto logicalOperator = SelectionLogicalOperator(predicate);

        const auto inputSchemas = SerializedUtils::deserializeSchemas(data.inputSchemas);
        return logicalOperator.withInferredSchema(inputSchemas);
    }
    throw UnknownLogicalOperator();
}
}
