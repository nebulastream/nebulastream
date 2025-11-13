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

#include <Operators/SequenceLogicalOperator.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

SequenceLogicalOperator::SequenceLogicalOperator() = default;

std::string_view SequenceLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool SequenceLogicalOperator::operator==(const SequenceLogicalOperator& rhs) const
{
    return getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas();
};

std::string SequenceLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SEQUENCE(opId: {})", opId);
    }
    return fmt::format("SEQUENCE()");
}

SequenceLogicalOperator SequenceLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(!inputSchemas.empty(), "Selection should have at least one input");

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Selection operator");
        }
    }

    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;
    return copy;
}
SequenceLogicalOperator SequenceLogicalOperator::setInputSchemas(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    copy.inputSchema = inputSchemas.at(0);
    return copy;
}
SequenceLogicalOperator SequenceLogicalOperator::setOutputSchema(const Schema& outputSchema) const
{
    auto copy = *this;
    copy.outputSchema = outputSchema;
    return copy;
}

TraitSet SequenceLogicalOperator::getTraitSet() const
{
    return {traitSet};
}

SequenceLogicalOperator SequenceLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

SequenceLogicalOperator SequenceLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SequenceLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema SequenceLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> SequenceLogicalOperator::getChildren() const
{
    return children;
}

void SequenceLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    auto inputs = getInputSchemas();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], schProto);
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterSequenceLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    auto logicalOperator = SequenceLogicalOperator();
    return logicalOperator.withInferredSchema(arguments.inputSchemas);
}
}
