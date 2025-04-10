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
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Watermark/TimeFunction.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <Watermark/EventTimeWatermarkAssignerPhysicalOperator.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalEventTimeWatermarkAssigner.hpp>

namespace NES::Optimizer
{

RewriteRuleResult LowerToPhysicalEventTimeWatermarkAssigner::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<EventTimeWatermarkAssignerLogicalOperator>(), "Expected a EventTimeWatermarkAssigner");
    auto assigner = logicalOperator.get<EventTimeWatermarkAssignerLogicalOperator>();
    auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(assigner.onField);
    auto physicalOperator = EventTimeWatermarkAssignerPhysicalOperator(EventTimeFunction(physicalFunction, assigner.unit));
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(physicalOperator, logicalOperator.getInputSchemas()[0], logicalOperator.getOutputSchema());
    return {wrapper, {wrapper}};
}

std::unique_ptr<Optimizer::AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterEventTimeWatermarkAssignerRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<NES::Optimizer::LowerToPhysicalEventTimeWatermarkAssigner>(argument.conf);
}
}
