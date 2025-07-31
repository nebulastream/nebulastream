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
#include <LoweringRules/LowerToPhysical/LowerToPhysicalEventTimeWatermarkAssigner.hpp>

#include <memory>

#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FunctionProvider.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <Watermark/EventTimeWatermarkAssignerPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalEventTimeWatermarkAssigner::apply(LogicalOperator logicalOperator)
{
    auto assignerOp = logicalOperator.getAs<EventTimeWatermarkAssignerLogicalOperator>();
    const auto traitSet = logicalOperator.getTraitSet();

    const auto memoryLayoutTypeTrait = traitSet.get<MemoryLayoutTypeTrait>();
    const auto memoryLayoutType = memoryLayoutTypeTrait->memoryLayout;

    const Schema<QualifiedUnboundField, Ordered> outputSchema = createPhysicalOutputSchema(traitSet);
    const Schema<QualifiedUnboundField, Ordered> inputSchema = createPhysicalOutputSchema(assignerOp->getChild().getTraitSet());

    const auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(assignerOp->getOnField());
    auto physicalOperator = EventTimeWatermarkAssignerPhysicalOperator(EventTimeFunction(physicalFunction, assignerOp->getUnit()));

    const auto wrapper
        = std::make_shared<PhysicalOperatorWrapper>(physicalOperator, inputSchema, outputSchema, memoryLayoutType, memoryLayoutType);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterEventTimeWatermarkAssignerLoweringRule(LoweringRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalEventTimeWatermarkAssigner>(argument.conf);
}
}
