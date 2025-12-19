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

#include <Operators/UnionLogicalOperator.hpp>

#include <algorithm>
#include <functional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

UnionLogicalOperator::UnionLogicalOperator() = default;

std::string_view UnionLogicalOperator::getName() noexcept
{
    return NAME;
}

bool UnionLogicalOperator::operator==(const UnionLogicalOperator& rhs) const
{
    return getInputSchemas() == rhs.getInputSchemas() && getOutputSchema() == rhs.getOutputSchema() && getTraitSet() == rhs.getTraitSet();
}

std::string UnionLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        if (!outputSchema.hasFields())
        {
            return fmt::format("UnionWith(OpId: {}, {}, traitSet: {})", id, outputSchema, traitSet.explain(verbosity));
        }

        return fmt::format("UnionWith(OpId: {})", id);
    }
    return "UnionWith";
}

UnionLogicalOperator UnionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    PRECONDITION(!inputSchemas.empty(), "Union expects at least one child");
    auto copy = *this;

    /// If all input schemas are identical including the source qualifier the union keeps the source qualifier.
    /// Compares adjacent elements and returs the first pair where they are not equal.
    /// If all of them are equal this returns the end iterator
    const auto allSchemasEqualWithQualifier = std::ranges::adjacent_find(inputSchemas, std::ranges::not_equal_to{}) == inputSchemas.end();
    if (!allSchemasEqualWithQualifier)
    {
        auto inputsWithoutSourceQualifier = inputSchemas | std::views::transform(withoutSourceQualifier);
        const auto allSchemasEqualWithoutQualifier
            = std::ranges::adjacent_find(inputsWithoutSourceQualifier, std::ranges::not_equal_to{}) == inputsWithoutSourceQualifier.end();
        if (!allSchemasEqualWithoutQualifier)
        {
            throw CannotInferSchema("Missmatch between union schemas.\n{}", fmt::join(inputSchemas, "\n"));
        }
        /// drop the qualifier
        copy.outputSchema = withoutSourceQualifier(inputSchemas[0]);
    }
    else
    {
        ///Input schema will be the output schema
        copy.outputSchema = inputSchemas[0];
    }

    copy.inputSchemas = std::move(inputSchemas);
    return copy;
}

TraitSet UnionLogicalOperator::getTraitSet() const
{
    return traitSet;
}

UnionLogicalOperator UnionLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

UnionLogicalOperator UnionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> UnionLogicalOperator::getInputSchemas() const
{
    return inputSchemas;
};

Schema UnionLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> UnionLogicalOperator::getChildren() const
{
    return children;
}

void UnionLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    for (const auto& inputSchema : getInputSchemas())
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputSchema, schProto);
    }


    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

UnionLogicalOperator UnionLogicalOperator::setInputSchemas(std::vector<Schema> inputSchemas) const
{
    if (inputSchemas.empty())
    {
        throw UnknownException("Expected at least input schema");
    }
    auto copy = *this;
    copy.inputSchemas = std::move(inputSchemas);
    return copy;
}

UnionLogicalOperator UnionLogicalOperator::setOutputSchema(const Schema& outputSchema) const
{
    auto copy = *this;
    copy.outputSchema.appendFieldsFromOtherSchema(outputSchema);
    return copy;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterUnionLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    auto logicalOperator = UnionLogicalOperator();
    if (arguments.inputSchemas.empty())
    {
        throw CannotDeserialize("Union expects at least one child but got {} inputSchemas!", arguments.inputSchemas.size());
    }
    auto logicalOp = logicalOperator.setInputSchemas(std::move(arguments.inputSchemas)).setOutputSchema(arguments.outputSchema);
    return logicalOp;
}

}
