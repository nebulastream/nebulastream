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

#include <API/Schema.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedMatrixJoinRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

DistributedMatrixJoinRule::DistributedMatrixJoinRule(Configurations::OptimizerConfiguration configuration, TopologyPtr topology)
    : topology(topology), configuration(configuration) {}

DistributedMatrixJoinRulePtr DistributedMatrixJoinRule::create(Configurations::OptimizerConfiguration configuration,
                                                               TopologyPtr topology) {
    return std::make_shared<DistributedMatrixJoinRule>(DistributedMatrixJoinRule(configuration, topology));
}

QueryPlanPtr DistributedMatrixJoinRule::apply(QueryPlanPtr queryPlan) {
    NES_DEBUG("DistributedMatrixJoinRule: Plan before replacement\n{}", queryPlan->toString());
    auto joinOps = queryPlan->getOperatorByType<LogicalJoinOperator>();
    if (!joinOps.empty()) {
        NES_DEBUG("DistributedMatrixJoinRule::apply: found {} join operators", joinOps.size());
        for (const LogicalJoinOperatorPtr& joinOp : joinOps) {
            NES_DEBUG("DistributedMatrixJoinRule::apply: join operator {}", joinOp->toString());
            auto parents = joinOp->getParents();
            auto leftOps = joinOp->getLeftOperators();
            auto rightOps = joinOp->getRightOperators();
            for (const auto& leftOp : leftOps) {
                for (const auto& rightOp : rightOps) {
                    // create join replicas for each lhs/rhs combination
                    LogicalJoinOperatorPtr newJoin =
                        LogicalOperatorFactory::createJoinOperator(joinOp->getJoinDefinition())->as<LogicalJoinOperator>();
                    newJoin->addChild(leftOp);
                    newJoin->addChild(rightOp);
                    for (const auto& parent : parents) {
                        parent->addChild(newJoin);
                    }
                }
            }
            // replace old central join with the new join replicas
            joinOp->removeAllParent();
            joinOp->removeChildren();
        }
    } else {
        NES_DEBUG("DistributedMatrixJoinRule: No join operator in query");
    }

    NES_DEBUG("DistributedMatrixJoinRule: Plan after replacement\n{}", queryPlan->toString());

    return queryPlan;
}

}// namespace NES::Optimizer
