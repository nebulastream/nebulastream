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

#include <utility>
#include <API/Functions/ArithmeticalFunctions.hpp>
#include <API/Functions/Functions.hpp>
#include <Functions/ArithmeticalFunctions/AddFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/CeilFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/DivFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/ExpFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/FloorFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/ModFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/MulFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/RoundFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/SqrtFunctionNode.hpp>
#include <Functions/ArithmeticalFunctions/SubFunctionNode.hpp>
#include <Functions/ConstantValueFunctionNode.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

/// calls of binary operators with two FunctionNodes
FunctionNodePtr operator+(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return AddFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator-(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return SubFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator*(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return MulFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator/(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return DivFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr operator%(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return ModFunctionNode::create(std::move(leftExp), std::move(rightExp));
}

FunctionNodePtr MOD(FunctionNodePtr leftExp, FunctionNodePtr rightExp)
{
    return std::move(leftExp) % std::move(rightExp);
}

FunctionNodePtr SQRT(const FunctionNodePtr& exp)
{
    return SqrtFunctionNode::create(exp);
}

FunctionNodePtr EXP(const FunctionNodePtr& exp)
{
    return ExpFunctionNode::create(exp);
}

FunctionNodePtr ROUND(const FunctionNodePtr& exp)
{
    return RoundFunctionNode::create(exp);
}

FunctionNodePtr CEIL(const FunctionNodePtr& exp)
{
    return CeilFunctionNode::create(exp);
}

FunctionNodePtr FLOOR(const FunctionNodePtr& exp)
{
    return FloorFunctionNode::create(exp);
}

FunctionNodePtr operator++(FunctionNodePtr leftExp)
{
    return std::move(leftExp)
        + ConstantValueFunctionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

FunctionNodePtr operator--(FunctionNodePtr leftExp)
{
    return std::move(leftExp)
        - ConstantValueFunctionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

FunctionNodePtr operator++(FunctionNodePtr leftExp, int)
{
    return std::move(leftExp)
        + ConstantValueFunctionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

FunctionNodePtr operator--(FunctionNodePtr leftExp, int)
{
    return std::move(leftExp)
        - ConstantValueFunctionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

/// calls of Binary operators with one or two FunctionItems
/// leftExp: item, rightExp: node
FunctionNodePtr operator+(FunctionItem leftExp, FunctionNodePtr rightExp)
{
    return leftExp.getFunctionNode() + std::move(rightExp);
}

FunctionNodePtr operator-(FunctionItem leftExp, FunctionNodePtr rightExp)
{
    return leftExp.getFunctionNode() - std::move(rightExp);
}

FunctionNodePtr operator*(FunctionItem leftExp, FunctionNodePtr rightExp)
{
    return leftExp.getFunctionNode() * std::move(rightExp);
}

FunctionNodePtr operator/(FunctionItem leftExp, FunctionNodePtr rightExp)
{
    return leftExp.getFunctionNode() / std::move(rightExp);
}

FunctionNodePtr operator%(FunctionItem leftExp, FunctionNodePtr rightExp)
{
    return leftExp.getFunctionNode() % std::move(rightExp);
}

FunctionNodePtr MOD(FunctionItem leftExp, FunctionNodePtr rightExp)
{
    return leftExp.getFunctionNode() % std::move(rightExp);
}

FunctionNodePtr POWER(FunctionItem leftExp, FunctionNodePtr rightExp)
{
    return POWER(leftExp.getFunctionNode(), std::move(rightExp));
}

/// leftExp: node, rightExp: item
FunctionNodePtr operator+(FunctionNodePtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) + rightExp.getFunctionNode();
}

FunctionNodePtr operator-(FunctionNodePtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) - rightExp.getFunctionNode();
}

FunctionNodePtr operator*(FunctionNodePtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) * rightExp.getFunctionNode();
}

FunctionNodePtr operator/(FunctionNodePtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) / rightExp.getFunctionNode();
}

FunctionNodePtr operator%(FunctionNodePtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) % rightExp.getFunctionNode();
}

FunctionNodePtr MOD(FunctionNodePtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) % rightExp.getFunctionNode();
}

FunctionNodePtr POWER(FunctionNodePtr leftExp, FunctionItem rightExp)
{
    return POWER(std::move(leftExp), rightExp.getFunctionNode());
}

/// leftExp: item, rightWxp: item
FunctionNodePtr operator+(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getFunctionNode() + rightExp.getFunctionNode();
}

FunctionNodePtr operator-(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getFunctionNode() - rightExp.getFunctionNode();
}

FunctionNodePtr operator*(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getFunctionNode() * rightExp.getFunctionNode();
}

FunctionNodePtr operator/(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getFunctionNode() / rightExp.getFunctionNode();
}

FunctionNodePtr operator%(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getFunctionNode() % rightExp.getFunctionNode();
}

FunctionNodePtr MOD(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getFunctionNode() % rightExp.getFunctionNode();
}

FunctionNodePtr POWER(FunctionItem leftExp, FunctionItem rightExp)
{
    return POWER(leftExp.getFunctionNode(), rightExp.getFunctionNode());
}

/// calls of Unary operators with FunctionItem
FunctionNodePtr ABS(FunctionItem exp)
{
    return ABS(exp.getFunctionNode());
}

FunctionNodePtr SQRT(FunctionItem exp)
{
    return SQRT(exp.getFunctionNode());
}

FunctionNodePtr EXP(FunctionItem exp)
{
    return EXP(exp.getFunctionNode());
}

FunctionNodePtr LN(FunctionItem exp)
{
    return LN(exp.getFunctionNode());
}

FunctionNodePtr LOG2(FunctionItem exp)
{
    return LOG2(exp.getFunctionNode());
}

FunctionNodePtr LOG10(FunctionItem exp)
{
    return LOG10(exp.getFunctionNode());
}

FunctionNodePtr SIN(FunctionItem exp)
{
    return SIN(exp.getFunctionNode());
}

FunctionNodePtr COS(FunctionItem exp)
{
    return COS(exp.getFunctionNode());
}

FunctionNodePtr RADIANS(FunctionItem exp)
{
    return RADIANS(exp.getFunctionNode());
}

FunctionNodePtr ROUND(FunctionItem exp)
{
    return ROUND(exp.getFunctionNode());
}

FunctionNodePtr CEIL(FunctionItem exp)
{
    return CEIL(exp.getFunctionNode());
}

FunctionNodePtr FLOOR(FunctionItem exp)
{
    return FLOOR(exp.getFunctionNode());
}

FunctionNodePtr operator++(FunctionItem exp)
{
    return ++exp.getFunctionNode();
}

FunctionNodePtr operator++(FunctionItem exp, int)
{
    return exp.getFunctionNode()++;
}

FunctionNodePtr operator--(FunctionItem exp)
{
    return --exp.getFunctionNode();
}

FunctionNodePtr operator--(FunctionItem exp, int)
{
    return exp.getFunctionNode()--;
}

}
