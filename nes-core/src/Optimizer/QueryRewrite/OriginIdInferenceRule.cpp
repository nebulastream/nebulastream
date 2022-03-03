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

#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Optimizer/QueryRewrite/OriginIdInferenceRule.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>

namespace NES::Optimizer {

OriginIdInferenceRule::OriginIdInferenceRule() {}

OriginIdInferenceRulePtr OriginIdInferenceRule::create() {
    return std::make_shared<OriginIdInferenceRule>(OriginIdInferenceRule());
}

QueryPlanPtr OriginIdInferenceRule::apply(QueryPlanPtr queryPlan) {
    uint64_t originIdCounter = 0;
    for (auto source : queryPlan->getSourceOperators()) {
        source->setOriginId(originIdCounter++);
    }

    // set origin id for window operator
    for (auto windowOperator : queryPlan->getOperatorByType<WindowOperatorNode>()) {
        windowOperator->setOriginId(originIdCounter++);
    }

    // for (auto joinOperator :   queryPlan->getOperatorByType<JoinLogicalOperatorNode>()) {
    //    joinOperator->getJoinDefinition().se
    // }

    // propagate origin ids through the complete query plan
    for (auto sink : queryPlan->getSinkOperators()) {
        sink->inferInputOrigins();
    }
    return queryPlan;
}

}// namespace NES::Optimizer