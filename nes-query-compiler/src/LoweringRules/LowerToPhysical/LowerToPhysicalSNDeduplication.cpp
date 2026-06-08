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


#include <LoweringRules/LowerToPhysical/LowerToPhysicalSNDeduplication.hpp>

#include <memory>
#include <FaultTolerance/SNDeduplicationOperatorHandler.hpp>
#include <FaultTolerance/SNDeduplicationPhysicalOperator.hpp>
#include <Functions/FunctionProvider.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/FaultTolerance/SNDeduplicationLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>


namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalSNDeduplication::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<SNDeduplicationLogicalOperator>(), "Expected a SelectionLogicalOperator");
    const auto lop = logicalOperator.getAs<SNDeduplicationLogicalOperator>();
    auto handlerId = getNextOperatorHandlerId();
    const auto inputOriginIds
    = lop.getChildren()
    | std::views::transform(
          [](const auto& child)
          {
              auto childOutputOriginIds = getTrait<OutputOriginIdsTrait>(child.getTraitSet());
              PRECONDITION(childOutputOriginIds.has_value(), "Expected the outputOriginIds trait of the child to be set");
              return childOutputOriginIds.value().get();
          })
    | std::views::join | std::ranges::to<std::vector<OriginId>>();

    auto handler = std::make_shared<SNDeduplicationOperatorHandler>(lop->getFilePath(), inputOriginIds);
    auto physicalOperator = SNDeduplicationPhysicalOperator(handlerId, lop->getFilePath());

    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        logicalOperator.getInputSchemas()[0],
        logicalOperator.getOutputSchema(),
        memoryLayoutType,
        memoryLayoutType,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
};

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterSNDeduplicationLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalSNDeduplication>(argument.conf);
}
}
