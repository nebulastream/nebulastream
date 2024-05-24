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

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/CeilExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/ExpExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/FloorExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/ModExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/RoundExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/SqrtExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/Functions/FunctionExpressionNode.hpp>
#include <utility>

namespace NES {

// calls of binary operators with two ExpressionNodes
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
    return FunctionExpression::create(DataTypeFactory::createUndefined(), "power", {leftExp, rightExp});
}

// calls of unary operators with ExpressionNode
ExpressionNodePtr ABS(const ExpressionNodePtr& exp) {
    return FunctionExpression::create(DataTypeFactory::createUndefined(), "abs", {exp});
}

ExpressionNodePtr SQRT(const ExpressionNodePtr& exp) { return SqrtExpressionNode::create(exp); }

ExpressionNodePtr EXP(const ExpressionNodePtr& exp) { return ExpExpressionNode::create(exp); }

ExpressionNodePtr LN(const ExpressionNodePtr& exp) {
    return FunctionExpression::create(DataTypeFactory::createUndefined(), "ln", {exp});
}

ExpressionNodePtr LOG2(const ExpressionNodePtr& exp) {
    return FunctionExpression::create(DataTypeFactory::createUndefined(), "log2", {exp});
}

ExpressionNodePtr LOG10(const ExpressionNodePtr& exp) {
    return FunctionExpression::create(DataTypeFactory::createUndefined(), "log10", {exp});
}

ExpressionNodePtr SIN(const ExpressionNodePtr& exp) {
    return FunctionExpression::create(DataTypeFactory::createUndefined(), "sin", {exp});
}

ExpressionNodePtr COS(const ExpressionNodePtr& exp) {
    return FunctionExpression::create(DataTypeFactory::createUndefined(), "cos", {exp});
}

ExpressionNodePtr RADIANS(const ExpressionNodePtr& exp) {
    return FunctionExpression::create(DataTypeFactory::createUndefined(), "radians", {exp});
}

ExpressionNodePtr ROUND(const ExpressionNodePtr& exp) { return RoundExpressionNode::create(exp); }

ExpressionNodePtr CEIL(const ExpressionNodePtr& exp) { return CeilExpressionNode::create(exp); }

ExpressionNodePtr FLOOR(const ExpressionNodePtr& exp) { return FloorExpressionNode::create(exp); }

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
// leftExp: item, rightExp: node
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

// leftExp: node, rightExp: item
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

// leftExp: item, rightWxp: item
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

ExpressionNodePtr SQRT(ExpressionItem exp) { return SQRT(exp.getExpressionNode()); }

ExpressionNodePtr EXP(ExpressionItem exp) { return EXP(exp.getExpressionNode()); }

ExpressionNodePtr LN(ExpressionItem exp) { return LN(exp.getExpressionNode()); }

ExpressionNodePtr LOG2(ExpressionItem exp) { return LOG2(exp.getExpressionNode()); }

ExpressionNodePtr LOG10(ExpressionItem exp) { return LOG10(exp.getExpressionNode()); }

ExpressionNodePtr SIN(ExpressionItem exp) { return SIN(exp.getExpressionNode()); }

ExpressionNodePtr COS(ExpressionItem exp) { return COS(exp.getExpressionNode()); }

ExpressionNodePtr RADIANS(ExpressionItem exp) { return RADIANS(exp.getExpressionNode()); }

ExpressionNodePtr ROUND(ExpressionItem exp) { return ROUND(exp.getExpressionNode()); }

ExpressionNodePtr CEIL(ExpressionItem exp) { return CEIL(exp.getExpressionNode()); }

ExpressionNodePtr FLOOR(ExpressionItem exp) { return FLOOR(exp.getExpressionNode()); }

ExpressionNodePtr operator++(ExpressionItem exp) { return ++exp.getExpressionNode(); }

ExpressionNodePtr operator++(ExpressionItem exp, int) { return exp.getExpressionNode()++; }

ExpressionNodePtr operator--(ExpressionItem exp) { return --exp.getExpressionNode(); }

ExpressionNodePtr operator--(ExpressionItem exp, int) { return exp.getExpressionNode()--; }

}// namespace NES
