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
#include <MapPhysicalOperator.hpp>
#include <Operators/MapLogicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalIngestionTimeWatermarkAssigner.hpp>

namespace NES::Optimizer
{

RewriteRuleResult LowerToPhysicalIngestionTimeWatermarkAssigner::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<MapLogicalOperator>(), "Expected a IngestionTimeWatermarkAssigner");
    auto map = logicalOperator.get<MapLogicalOperator>();
    auto function = map.getMapFunction().getAssignment();
    auto fieldName = map.getMapFunction().getField().getFieldName();
    auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(function);
    auto physicalOperator = MapPhysicalOperator(fieldName, physicalFunction);
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(physicalOperator, logicalOperator.getInputSchemas()[0], logicalOperator.getOutputSchema());
    return {wrapper, {wrapper}};
}

std::unique_ptr<Optimizer::AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterIngestionTimeWatermarkAssignerRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<NES::Optimizer::LowerToPhysicalIngestionTimeWatermarkAssigner>(argument.conf);
}
}
