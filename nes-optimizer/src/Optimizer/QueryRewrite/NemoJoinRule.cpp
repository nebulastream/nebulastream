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
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDefinition.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/NemoJoinRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer {

NemoJoinRule::NemoJoinRule(Configurations::OptimizerConfiguration configuration, TopologyPtr topology)
    : topology(topology), configuration(configuration) {}

NemoJoinRulePtr NemoJoinRule::create(Configurations::OptimizerConfiguration configuration, TopologyPtr topology) {
    return std::make_shared<NemoJoinRule>(NemoJoinRule(configuration, topology));
}

QueryPlanPtr NemoJoinRule::apply(QueryPlanPtr queryPlan) {
    NES_DEBUG("NemoJoinRule: Plan before replacement\n{}", queryPlan->toString());
    auto joinOps = queryPlan->getOperatorByType<JoinLogicalOperatorNode>();
    if (!joinOps.empty()) {
        NES_DEBUG("NemoJoinRule::apply: found {} join operators", joinOps.size());
        for (const JoinLogicalOperatorNodePtr& joinOp : joinOps) {
            NES_DEBUG("NemoJoinRule::apply: join operator {}", joinOp->toString());
            auto parents = joinOp->getParents();
            auto leftOps = joinOp->getLeftOperators();
            auto rightOps = joinOp->getRightOperators();
            for (auto leftOp : leftOps) {
                for (auto rightOp : rightOps) {
                    JoinLogicalOperatorNodePtr newJoin =
                        LogicalOperatorFactory::createJoinOperator(joinOp->getJoinDefinition())->as<JoinLogicalOperatorNode>();
                    newJoin->addChild(leftOp);
                    newJoin->addChild(rightOp);
                    for (auto parent : parents) {
                        parent->addChild(newJoin);
                    }
                }
            }
            joinOp->removeAllParent();
            joinOp->removeChildren();
        }
    } else {
        NES_DEBUG("NemoJoinRule: No join operator in query");
    }

    NES_DEBUG("NemoJoinRule: Plan after replacement\n{}", queryPlan->toString());

    return queryPlan;
}

}// namespace NES::Optimizer
