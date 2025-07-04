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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalUnion.hpp>

#include <memory>
#include <optional>
#include <ranges>
#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <UnionPhysicalOperator.hpp>
#include <UnionRenamePhysicalOperator.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalUnion::apply(LogicalOperator logicalOperator)
{
    const auto source = logicalOperator.get<UnionLogicalOperator>();
    auto inputSchemas = logicalOperator.getInputSchemas();
    auto outputSchema = logicalOperator.getOutputSchema();

    PRECONDITION(logicalOperator.tryGet<UnionLogicalOperator>(), "Expected a UnionLogicalOperator");

    auto renames = inputSchemas
        | std::views::transform(
                       [&](const auto& schema)
                       {
                           return std::make_shared<PhysicalOperatorWrapper>(
                               UnionRenamePhysicalOperator(schema.getFieldNames(), outputSchema.getFieldNames()),
                               schema,
                               outputSchema,
                               PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
                       })
        | std::ranges::to<std::vector>();

    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        UnionPhysicalOperator(),
        source.getOutputSchema(),
        logicalOperator.getOutputSchema(),
        std::nullopt,
        std::nullopt,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE,
        renames);

    return {.root = wrapper, .leafs = renames};
}

RewriteRuleRegistryReturnType RewriteRuleGeneratedRegistrar::RegisterUnionRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalUnion>(argument.conf);
}
}
