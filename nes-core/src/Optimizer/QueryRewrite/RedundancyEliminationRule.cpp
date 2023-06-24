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

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/RedundancyEliminationRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <queue>

namespace NES::Optimizer {

RedundancyEliminationRulePtr RedundancyEliminationRule::create() { return std::make_shared<RedundancyEliminationRule>(RedundancyEliminationRule()); }

RedundancyEliminationRule::RedundancyEliminationRule() = default;

QueryPlanPtr RedundancyEliminationRule::apply(QueryPlanPtr queryPlan) {

    NES_INFO2("Applying FilterPushDownRule to query {}", queryPlan->toString());
    auto filters = queryPlan->getOperatorByType<FilterLogicalOperatorNode>();
    for (auto& filter : filters){
        const ExpressionNodePtr filterPredicate = filter->getPredicate();
        ExpressionNodePtr updatedPredicate = constantFolding(updatedPredicate);
        updatedPredicate = arithmeticSimplification(updatedPredicate);
        updatedPredicate = conjunctionDisjunctionSimplification(updatedPredicate);
    }
    return queryPlan;
}

NES::ExpressionNodePtr RedundancyEliminationRule::constantFolding(const ExpressionNodePtr& predicate) {
    // TODO
    return predicate;
}

NES::ExpressionNodePtr RedundancyEliminationRule::arithmeticSimplification(const NES::ExpressionNodePtr& predicate) {
    // TODO:
    return predicate;
}

NES::ExpressionNodePtr RedundancyEliminationRule::conjunctionDisjunctionSimplification(const NES::ExpressionNodePtr& predicate) {
    // TODO
    return predicate;
}


}// namespace NES::Optimizer
