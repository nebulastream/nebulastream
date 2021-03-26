/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Operators/LogicalOperators/Arity/BinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/BinaryOperatorSortRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

BinaryOperatorSortRulePtr BinaryOperatorSortRule::create() {
    return std::make_shared<BinaryOperatorSortRule>(BinaryOperatorSortRule());
}

BinaryOperatorSortRule::BinaryOperatorSortRule() { NES_DEBUG("BinaryOperatorSortRule()"); };

QueryPlanPtr BinaryOperatorSortRule::apply(QueryPlanPtr queryPlanPtr) {
    auto joinOperators = queryPlanPtr->getOperatorByType<JoinLogicalOperatorNode>();
    for (auto joinOperator : joinOperators) {
        sortChildren(joinOperator);
    }

    auto unionOperators = queryPlanPtr->getOperatorByType<UnionLogicalOperatorNode>();
    for (auto unionOperator : unionOperators) {
        sortChildren(unionOperator);
    }
    return queryPlanPtr;
}

void BinaryOperatorSortRule::sortChildren(BinaryOperatorNodePtr binaryOperator) {
    auto children = binaryOperator->getChildren();
    NES_ASSERT(children.size() == 2, "Binary operator should have only 2 children");

    auto leftChild = children[0];
    auto rightChild = children[1];

    auto leftInputSchema = binaryOperator->getLeftInputSchema();
    auto leftQualifierName = leftInputSchema->getQualifierNameForSystemGeneratedFields();
    auto rightInputSchema = binaryOperator->getRightInputSchema();
    auto rightQualifierName = rightInputSchema->getQualifierNameForSystemGeneratedFields();

    if (leftQualifierName.compare(rightQualifierName) > 0) {
        binaryOperator->removeChild(leftChild);
        binaryOperator->addChild(leftChild);
        binaryOperator->setRightInputSchema(leftInputSchema);
        binaryOperator->setLeftInputSchema(rightInputSchema);
    }
}
}// namespace NES