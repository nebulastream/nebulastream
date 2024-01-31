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

#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperatorNode.hpp>
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

DistributedWindowRule::DistributedWindowRule(Configurations::OptimizerConfiguration configuration)
    : performDistributedWindowOptimization(configuration.performDistributedWindowOptimization),
      windowDistributionChildrenThreshold(configuration.distributedWindowChildThreshold),
      windowDistributionCombinerThreshold(configuration.distributedWindowCombinerThreshold) {
    if (performDistributedWindowOptimization) {
        NES_DEBUG("Create DistributedWindowRule with distributedWindowChildThreshold: {} distributedWindowCombinerThreshold: {}",
                  windowDistributionChildrenThreshold,
                  windowDistributionCombinerThreshold);
    } else {
        NES_DEBUG("Disable DistributedWindowRule");
    }
};

DistributeWindowRulePtr DistributedWindowRule::create(Configurations::OptimizerConfiguration configuration) {
    return std::make_shared<DistributedWindowRule>(DistributedWindowRule(configuration));
}

QueryPlanPtr DistributedWindowRule::apply(QueryPlanPtr queryPlan) {
    NES_DEBUG("DistributedWindowRule: Apply DistributedWindowRule.");
    NES_DEBUG("DistributedWindowRule::apply: plan before replace {}", queryPlan->toString());

    NES_DEBUG("DistributedWindowRule::apply: plan after replace {}", queryPlan->toString());
    return queryPlan;
}

void DistributedWindowRule::createDistributedWindowOperator(const WindowOperatorNodePtr& logicalWindowOperator,
                                                            const QueryPlanPtr& queryPlan) {
    // To distribute the window operator we replace the current window operator with 1 WindowComputationOperator (performs the final aggregate)
    // and n SliceCreationOperators.
    // To this end, we have to a the window definitions in the following way:
    // The SliceCreation consumes input and outputs data in the schema: {startTs, endTs, keyField, value}
    // The WindowComputation consumes that schema and outputs data in: {startTs, endTs, keyField, outputAggField}
    // First we prepare the final WindowComputation operator:

    //if window has more than 4 edges, we introduce a combiner

    NES_DEBUG("DistributedWindowRule::apply: introduce distributed window operator for window {}",
              logicalWindowOperator->toString());
    auto windowDefinition = logicalWindowOperator->getWindowDefinition();
    auto windowType = windowDefinition->getWindowType();
    auto windowAggregation = windowDefinition->getWindowAggregation();
    auto keyField = windowDefinition->getKeys();
    auto allowedLateness = windowDefinition->getAllowedLateness();
    // For the final window computation we have to change copy aggregation function and manipulate the fields we want to aggregate.
    auto windowComputationAggregation = windowAggregation[0]->copy();
    //    windowComputationAggregation->on()->as<FieldAccessExpressionNode>()->setFieldName("value");

    Windowing::LogicalWindowDefinitionPtr windowDef;
    if (logicalWindowOperator->getWindowDefinition()->isKeyed()) {
        windowDef = Windowing::LogicalWindowDefinition::create(keyField,
                                                               {windowComputationAggregation},
                                                               windowType,
                                                               allowedLateness);

    } else {
        windowDef = Windowing::LogicalWindowDefinition::create({windowComputationAggregation},
                                                               windowType,
                                                               allowedLateness);
    }
    NES_DEBUG("DistributedWindowRule::apply: created logical window definition for computation operator{}",
              windowDef->toString());

    auto assignerOp = queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>();
    NES_ASSERT(assignerOp.size() > 1, "at least one assigner has to be there");
}

}// namespace NES::Optimizer
