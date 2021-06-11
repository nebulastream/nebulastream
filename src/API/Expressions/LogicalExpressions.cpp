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

#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <utility>

namespace NES {

ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return OrExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return AndExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return EqualsExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return NegateExpressionNode::create(EqualsExpressionNode::create(std::move(leftExp), std::move(rightExp)));
}

ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return LessEqualsExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return LessExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return GreaterEqualsExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return GreaterExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator!(ExpressionNodePtr exp) { return NegateExpressionNode::create(std::move(exp)); }
ExpressionNodePtr operator!(ExpressionItem exp) { return !exp.getExpressionNode(); }

}// namespace NES