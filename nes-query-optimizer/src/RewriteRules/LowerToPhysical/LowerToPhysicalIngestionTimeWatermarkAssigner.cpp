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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalIngestionTimeWatermarkAssigner.hpp>

#include <memory>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Watermark/IngestionTimeWatermarkAssignerPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalIngestionTimeWatermarkAssigner::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>(), "Expected a IngestionTimeWatermarkAssigner");
    auto physicalOperator = IngestionTimeWatermarkAssignerPhysicalOperator(IngestionTimeFunction());
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator, logicalOperator.getInputSchemas()[0], logicalOperator.getOutputSchema(), memoryLayoutType, memoryLayoutType);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterIngestionTimeWatermarkAssignerRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalIngestionTimeWatermarkAssigner>(argument.conf);
}
}
