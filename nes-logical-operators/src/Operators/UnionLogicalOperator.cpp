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

std::string_view UnionLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string UnionLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        if (!outputSchema.hasFields())
        {
            return fmt::format("UnionWith(OpId: {}, {})", id, outputSchema);
        }

        return fmt::format("UnionWith(OpId: {})", id);
    }
    return "UnionWith";
}

LogicalOperator UnionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
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

LogicalOperator LogicalOperatorGeneratedRegistrar::RegisterUnionLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    return UnionLogicalOperator();
}

}

