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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalSelection.hpp>

#include <memory>

#include <Functions/FunctionProvider.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <SelectionPhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalSelection::apply(LogicalOperator logicalOperator)
{
    const auto selection = logicalOperator.getAs<SelectionLogicalOperator>();
    const auto function = selection->getPredicate();
    const auto func
        = QueryCompilation::FunctionProvider::lowerFunction(function, *selection->getChild()->getTraitSet().get<FieldMappingTrait>());
    const auto traitSet = logicalOperator.getTraitSet();

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(selection->getChild()->getTraitSet());

    auto physicalOperator = SelectionPhysicalOperator(func);
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
};

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterSelectionLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSelection>(argument.conf);
}
}
