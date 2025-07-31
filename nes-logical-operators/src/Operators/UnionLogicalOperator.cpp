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
#include <cstdint>
#include <functional>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include "DataTypes/UnboundSchema.hpp"
#include "Schema/Field.hpp"

namespace NES
{

namespace
{
SchemaBase<UnboundFieldBase<1>, false> inferOutputSchema(const std::vector<LogicalOperator>& children)
{
    auto inputSchemas
        = children | std::views::transform([](const auto& child) { return child.getOutputSchema(); }) | std::ranges::to<std::vector>();
    auto inputSchemaSizes = inputSchemas | std::views::transform([](const auto& schema) { return std::ranges::size(schema); });

    if (std::ranges::adjacent_find(inputSchemaSizes, std::ranges::not_equal_to{}) != std::ranges::end(inputSchemaSizes))
    {
        throw CannotInferSchema("Union expects all children to have the same number of fields");
    }

    std::unordered_map<LogicalOperator, std::unordered_set<Field>> fieldMismatches;
    std::unordered_set<UnboundFieldBase<1>> commonFields
        = inputSchemas.at(0) | std::views::transform(Field::unbinder()) | std::ranges::to<std::unordered_set>();
    /// Check for mismatch and build intersection of all input schemas by unbound fields
    bool mismatch = false;
    for (const auto& inputSchema : inputSchemas)
    {
        auto unboundInputSchema = inputSchema | std::views::transform(Field::unbinder()) | std::ranges::to<std::unordered_set>();
        mismatch |= unboundInputSchema != commonFields;
        if (mismatch)
        {
            auto newCommonFields = commonFields;
            for (const auto& commonField : commonFields)
            {
                if (!unboundInputSchema.contains(commonField))
                {
                    newCommonFields.erase(commonField);
                }
            }
            commonFields = std::move(newCommonFields);
        }
    }

    if (mismatch)
    {
        for (const auto& child : children)
        {
            for (const auto unboundInputSchema = child->getOutputSchema(); const auto& inputField : unboundInputSchema)
            {
                if (!commonFields.contains(inputField.unbound()))
                {
                    fieldMismatches[child].emplace(inputField);
                }
            }
        }
        throw CannotInferSchema(
            "Union expects all children to have the same fields, but the common schema was {} and found these additional fields in "
            "children: {}",
            commonFields | std::ranges::to<std::vector>(),
            fmt::join(
                fieldMismatches
                    | std::views::transform([](const auto& childMismatch)
                                            { return fmt::format("{}: {}", childMismatch.first, childMismatch.second); }),
                ", "));
    }

    /// For some reason, c++ doesn't convert *std::ranges::begin(inputSchemas) into a range of fields correctly
    return unbind((inputSchemas | std::ranges::to<std::vector<Schema>>()).at(0));
}
}

UnionLogicalOperator::UnionLogicalOperator()
{
}

UnionLogicalOperator::UnionLogicalOperator(std::vector<LogicalOperator> children)
{
    PRECONDITION(!children.empty(), "Union expects at least one child");

    this->children = std::move(children);
    // Validate schema compatibility and infer output schema
    this->outputSchema = inferOutputSchema(this->children);
}

std::string_view UnionLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool UnionLogicalOperator::operator==(const UnionLogicalOperator& rhs) const
{
    return getOutputSchema() == rhs.getOutputSchema() && getTraitSet() == rhs.getTraitSet();
}

std::string UnionLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        if (outputSchema.has_value())
        {
            return fmt::format("UnionWith(OpId: {}, {}, traitSet: {})", id, outputSchema.value(), traitSet.explain(verbosity));
        }

        return fmt::format("UnionWith(OpId: {})", id, traitSet.explain(verbosity));
    }
    return "UnionWith";
}

UnionLogicalOperator UnionLogicalOperator::withInferredSchema() const
{
    PRECONDITION(!children.empty(), "Union expects at least one child");
    auto copy = *this;

    copy.children
        = children | std::views::transform([](const auto& child) { return child.withInferredSchema(); }) | std::ranges::to<std::vector>();

    copy.outputSchema = inferOutputSchema(copy.children);

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

Schema UnionLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> UnionLogicalOperator::getChildren() const
{
    return children;
}

void UnionLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperator LogicalOperatorGeneratedRegistrar::RegisterUnionLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    return TypedLogicalOperator<UnionLogicalOperator>{std::move(arguments.children)};
}

}

uint64_t std::hash<NES::UnionLogicalOperator>::operator()(const NES::UnionLogicalOperator&) const noexcept
{
    return 1214827;
}