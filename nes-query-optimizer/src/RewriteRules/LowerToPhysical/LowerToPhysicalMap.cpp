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
#include <LogicalOperators/MapOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalMap.hpp>
#include <MapPhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES::Optimizer
{

RewriteRuleResultSubgraph LowerToPhysicalMap::apply(Logical::Operator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<Logical::MapOperator>(), "Expected a MapLogicalOperator");
    auto map = logicalOperator.get<Logical::MapOperator>();
    auto function = map.getMapFunction().getAssignment();
    auto fieldName = map.getMapFunction().getField().getFieldName();
    auto physicalFunction = QueryCompilation::FunctionProvider::lowerFunction(function);
    auto physicalOperator = MapPhysicalOperator(fieldName, physicalFunction);
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator, logicalOperator.getInputSchemas()[0], logicalOperator.getOutputSchema());

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {wrapper, {leafes}};
}

std::unique_ptr<AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterMapRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<LowerToPhysicalMap>(argument.conf);
}
}
