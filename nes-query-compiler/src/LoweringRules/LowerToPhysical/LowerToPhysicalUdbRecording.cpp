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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalUdbRecording.hpp>

#include <memory>

#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UdbRecordingLogicalOperator.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <UdbRecordingPhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalUdbRecording::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<UdbRecordingLogicalOperator>(), "Expected a UdbRecordingLogicalOperator");
    const auto udbOp = logicalOperator.getAs<UdbRecordingLogicalOperator>();

    const auto inputSchema = logicalOperator.getInputSchemas()[0];
    const auto outputSchema = logicalOperator.getOutputSchema();
    const auto memoryLayoutTypeTrait = logicalOperator.getTraitSet().tryGet<MemoryLayoutTypeTrait>();
    PRECONDITION(memoryLayoutTypeTrait.has_value(), "Expected a memory layout type trait");
    const auto memoryLayoutType = memoryLayoutTypeTrait.value()->memoryLayout;

    auto physicalOperator = UdbRecordingPhysicalOperator(udbOp->getTraceName());
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    const std::vector leafs{wrapper};
    return {.root = wrapper, .leafs = leafs};
}

std::unique_ptr<AbstractLoweringRule>
LoweringRuleGeneratedRegistrar::RegisterUdbRecordingLoweringRule(LoweringRuleRegistryArguments) /// NOLINT
{
    return std::make_unique<LowerToPhysicalUdbRecording>();
}

}
