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

#include <Functions/FunctionProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalSelection.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <SelectionPhysicalOperator.hpp>

#include <DataTypes/Schema.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <EmitOperatorHandler.hpp>
#include <Utils.hpp>


namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalSelection::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<SelectionLogicalOperator>(), "Expected a SelectionLogicalOperator");
    auto selection = logicalOperator.get<SelectionLogicalOperator>();
    auto function = selection.getPredicate();
    auto func = QueryCompilation::FunctionProvider::lowerFunction(function);
    auto physicalOperator = SelectionPhysicalOperator(func);
    auto inputSchema = logicalOperator.getInputSchemas()[0];
    auto outputSchema = logicalOperator.getOutputSchema();




    /// if function left, right is field access, then use fieldname for outputschemas and swap after selection
    if (conf.layoutStrategy.getValue() != MemoryLayoutStrategy::LEGACY)
    {
        auto memoryLayoutTrait = getMemoryLayoutTypeTrait(logicalOperator);

        auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
               physicalOperator,
               logicalOperator.getInputSchemas()[0].withMemoryLayoutType(memoryLayoutTrait.targetLayoutType),
               logicalOperator.getOutputSchema().withMemoryLayoutType(memoryLayoutTrait.targetLayoutType),
               PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

        auto res = addSwapBeforeOperator(wrapper, memoryLayoutTrait, conf);

        auto root = res.first;
        auto leaf = res.second;
        std::vector leafes(logicalOperator.getChildren().size(), leaf);
        return {.root = root, .leafs = {leafes}};

    }

    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
       physicalOperator,
       logicalOperator.getInputSchemas()[0],
       logicalOperator.getOutputSchema(),
       PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
};

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterSelectionRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSelection>(argument.conf);
}
}
