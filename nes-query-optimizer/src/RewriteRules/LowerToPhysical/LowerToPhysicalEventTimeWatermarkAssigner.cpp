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
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalEventTimeWatermarkAssigner.hpp>
#include <Watermark/EventTimeWatermarkAssignerPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalEventTimeWatermarkAssigner::apply(LogicalOperator logicalOperator, QueryId)
{
    PRECONDITION(logicalOperator.tryGet<EventTimeWatermarkAssignerLogicalOperator>(), "Expected a EventTimeWatermarkAssigner");
    auto assigner = logicalOperator.get<EventTimeWatermarkAssignerLogicalOperator>();
    auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(assigner.onField);
    auto physicalOperator = EventTimeWatermarkAssignerPhysicalOperator(EventTimeFunction(physicalFunction, assigner.unit));
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator, logicalOperator.getInputSchemas()[0], logicalOperator.getOutputSchema());

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterEventTimeWatermarkAssignerRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<NES::LowerToPhysicalEventTimeWatermarkAssigner>(argument.conf);
}
}
