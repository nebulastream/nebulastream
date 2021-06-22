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

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AbsExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ModExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/PowExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <utility>
namespace NES {

ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return AddExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return SubExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return MulExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return DivExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr operator%(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return ModExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr MOD(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) { return std::move(leftExp) % std::move(rightExp); }

ExpressionNodePtr POWER(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return PowExpressionNode::create(std::move(leftExp), std::move(rightExp));
}

ExpressionNodePtr ABS(const ExpressionNodePtr& exp) {
    // TODO: implement the ABS value in the coming compiler (unary operators are not planned for the legacy system), then remove this error
    NES_FATAL_ERROR("ArithmeticExpressions: Unary expressions available in C++ API but not supported in the "
                    "legacy compiler: ABS("
                    << exp->toString() << ")");
    NES_NOT_IMPLEMENTED();
    return AbsExpressionNode::create(exp);
}

ExpressionNodePtr operator++(ExpressionNodePtr leftExp) {
    return std::move(leftExp)
        + ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

ExpressionNodePtr operator--(ExpressionNodePtr leftExp) {
    return std::move(leftExp)
        - ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

ExpressionNodePtr operator++(ExpressionNodePtr leftExp, int) {
    return std::move(leftExp)
        + ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

ExpressionNodePtr operator--(ExpressionNodePtr leftExp, int) {
    return std::move(leftExp)
        - ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

// calls of Binary operators with one or two ExpressionItems
ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() + std::move(rightExp);
}

ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() - std::move(rightExp);
}

ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() * std::move(rightExp);
}

ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() / std::move(rightExp);
}

ExpressionNodePtr operator%(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() % std::move(rightExp);
}

ExpressionNodePtr MOD(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() % std::move(rightExp);
}

ExpressionNodePtr POWER(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return POWER(leftExp.getExpressionNode(), std::move(rightExp));
}

ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return std::move(leftExp) + rightExp.getExpressionNode();
}

ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return std::move(leftExp) - rightExp.getExpressionNode();
}

ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return std::move(leftExp) * rightExp.getExpressionNode();
}

ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return std::move(leftExp) / rightExp.getExpressionNode();
}

ExpressionNodePtr operator%(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return std::move(leftExp) % rightExp.getExpressionNode();
}

ExpressionNodePtr MOD(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return std::move(leftExp) % rightExp.getExpressionNode();
}

ExpressionNodePtr POWER(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return POWER(std::move(leftExp), rightExp.getExpressionNode());
}

ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() + rightExp.getExpressionNode();
}

ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() - rightExp.getExpressionNode();
}

ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() * rightExp.getExpressionNode();
}

ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() / rightExp.getExpressionNode();
}

ExpressionNodePtr operator%(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() % rightExp.getExpressionNode();
}

ExpressionNodePtr MOD(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() % rightExp.getExpressionNode();
}

ExpressionNodePtr POWER(ExpressionItem leftExp, ExpressionItem rightExp) {
    return POWER(leftExp.getExpressionNode(), rightExp.getExpressionNode());
}

// calls of Unary operators with ExpressionItem
ExpressionNodePtr ABS(ExpressionItem exp) { return ABS(exp.getExpressionNode()); }

ExpressionNodePtr operator++(ExpressionItem exp) { return ++exp.getExpressionNode(); }

ExpressionNodePtr operator++(ExpressionItem exp, int) { return exp.getExpressionNode()++; }

ExpressionNodePtr operator--(ExpressionItem exp) { return --exp.getExpressionNode(); }

ExpressionNodePtr operator--(ExpressionItem exp, int) { return exp.getExpressionNode()--; }

}// namespace NES