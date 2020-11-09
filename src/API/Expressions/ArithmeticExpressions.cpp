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
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
namespace NES {

ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return AddExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return SubExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return DivExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return MulExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator++(ExpressionNodePtr leftExp) {
    return leftExp + ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

ExpressionNodePtr operator--(ExpressionNodePtr leftExp) {
    return leftExp - ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

ExpressionNodePtr operator++(ExpressionNodePtr leftExp, int) {
    return leftExp + ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

ExpressionNodePtr operator--(ExpressionNodePtr leftExp, int) {
    return leftExp - ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() + rightExp;
}

ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() - rightExp;
}

ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() / rightExp;
}

ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() * rightExp;
}

ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp + rightExp.getExpressionNode();
}

ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp - rightExp.getExpressionNode();
}

ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp / rightExp.getExpressionNode();
}

ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp * rightExp.getExpressionNode();
}

ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() + rightExp.getExpressionNode();
}

ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() - rightExp.getExpressionNode();
}

ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() / rightExp.getExpressionNode();
}

ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() * rightExp.getExpressionNode();
}

ExpressionNodePtr operator++(ExpressionItem exp) {
    return ++exp.getExpressionNode();
}

ExpressionNodePtr operator++(ExpressionItem exp, int) {
    return exp.getExpressionNode()++;
}

ExpressionNodePtr operator--(ExpressionItem exp) {
    return --exp.getExpressionNode();
}

ExpressionNodePtr operator--(ExpressionItem exp, int) {
    return exp.getExpressionNode()--;
}

}// namespace NES