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

namespace NES {

ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return OrExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return AndExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return EqualsExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return NegateExpressionNode::create(EqualsExpressionNode::create(leftExp, rightExp));
}

ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return LessEqualsExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return LessExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return GreaterEqualsExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return GreaterExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator!(ExpressionNodePtr exp) { return NegateExpressionNode::create(exp); }

ExpressionNodePtr operator||(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() || rightExp;
}

ExpressionNodePtr operator&&(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() && rightExp;
}

ExpressionNodePtr operator==(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() == rightExp;
}

ExpressionNodePtr operator!=(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() != rightExp;
}

ExpressionNodePtr operator<=(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() <= rightExp;
}

ExpressionNodePtr operator<(ExpressionItem leftExp, ExpressionNodePtr rightExp) { return leftExp.getExpressionNode() < rightExp; }

ExpressionNodePtr operator>=(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() >= rightExp;
}

ExpressionNodePtr operator>(ExpressionItem leftExp, ExpressionNodePtr rightExp) { return leftExp.getExpressionNode() > rightExp; }

ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp || rightExp.getExpressionNode();
}

ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp && rightExp.getExpressionNode();
}

ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp == rightExp.getExpressionNode();
}

ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp != rightExp.getExpressionNode();
}

ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp <= rightExp.getExpressionNode();
}

ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionItem rightExp) { return leftExp < rightExp.getExpressionNode(); }

ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp >= rightExp.getExpressionNode();
}

ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionItem rightExp) { return leftExp > rightExp.getExpressionNode(); }

ExpressionNodePtr operator||(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() || rightExp.getExpressionNode();
}

ExpressionNodePtr operator&&(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() && rightExp.getExpressionNode();
}

ExpressionNodePtr operator==(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() == rightExp.getExpressionNode();
}

ExpressionNodePtr operator!=(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() != rightExp.getExpressionNode();
}

ExpressionNodePtr operator<=(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() <= rightExp.getExpressionNode();
}

ExpressionNodePtr operator<(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() < rightExp.getExpressionNode();
}

ExpressionNodePtr operator>=(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() >= rightExp.getExpressionNode();
}

ExpressionNodePtr operator>(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() > rightExp.getExpressionNode();
}

ExpressionNodePtr operator!(ExpressionItem leftExp) { return !leftExp.getExpressionNode(); }

}// namespace NES