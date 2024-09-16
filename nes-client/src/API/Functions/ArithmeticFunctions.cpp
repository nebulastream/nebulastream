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
#include <Functions/ArithmeticalFunctions/NodeFunctionAdd.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionCeil.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionExp.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionFloor.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMod.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMul.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionRound.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionSqrt.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionSub.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

///NodeFunction calls of binary operators with two s
NodeFunctionPtr operator+(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp)
{
    return NodeFunctionAdd::create(std::move(leftExp), std::move(rightExp));
}

NodeFunctionPtr operator-(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp)
{
    return NodeFunctionSub::create(std::move(leftExp), std::move(rightExp));
}

NodeFunctionPtr operator*(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp)
{
    return NodeFunctionMul::create(std::move(leftExp), std::move(rightExp));
}

NodeFunctionPtr operator/(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp)
{
    return NodeFunctionDiv::create(std::move(leftExp), std::move(rightExp));
}

NodeFunctionPtr operator%(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp)
{
    return NodeFunctionMod::create(std::move(leftExp), std::move(rightExp));
}

NodeFunctionPtr MOD(NodeFunctionPtr leftExp, NodeFunctionPtr rightExp)
{
    return std::move(leftExp) % std::move(rightExp);
}

NodeFunctionPtr SQRT(const NodeFunctionPtr& exp)
{
    return NodeFunctionSqrt::create(exp);
}

NodeFunctionPtr EXP(const NodeFunctionPtr& exp)
{
    return NodeFunctionExp::create(exp);
}

NodeFunctionPtr ROUND(const NodeFunctionPtr& exp)
{
    return NodeFunctionRound::create(exp);
}

NodeFunctionPtr CEIL(const NodeFunctionPtr& exp)
{
    return NodeFunctionCeil::create(exp);
}

NodeFunctionPtr FLOOR(const NodeFunctionPtr& exp)
{
    return NodeFunctionFloor::create(exp);
}

NodeFunctionPtr operator++(NodeFunctionPtr leftExp)
{
    return std::move(leftExp)
        + NodeFunctionConstantValue::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

NodeFunctionPtr operator--(NodeFunctionPtr leftExp)
{
    return std::move(leftExp)
        - NodeFunctionConstantValue::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

NodeFunctionPtr operator++(NodeFunctionPtr leftExp, int)
{
    return std::move(leftExp)
        + NodeFunctionConstantValue::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

NodeFunctionPtr operator--(NodeFunctionPtr leftExp, int)
{
    return std::move(leftExp)
        - NodeFunctionConstantValue::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), /*value*/ "1"));
}

/// calls of Binary operators with one or two FunctionItems
/// leftExp: item, rightExp: node
NodeFunctionPtr operator+(FunctionItem leftExp, NodeFunctionPtr rightExp)
{
    return leftExp.getNodeFunction() + std::move(rightExp);
}

NodeFunctionPtr operator-(FunctionItem leftExp, NodeFunctionPtr rightExp)
{
    return leftExp.getNodeFunction() - std::move(rightExp);
}

NodeFunctionPtr operator*(FunctionItem leftExp, NodeFunctionPtr rightExp)
{
    return leftExp.getNodeFunction() * std::move(rightExp);
}

NodeFunctionPtr operator/(FunctionItem leftExp, NodeFunctionPtr rightExp)
{
    return leftExp.getNodeFunction() / std::move(rightExp);
}

NodeFunctionPtr operator%(FunctionItem leftExp, NodeFunctionPtr rightExp)
{
    return leftExp.getNodeFunction() % std::move(rightExp);
}

NodeFunctionPtr MOD(FunctionItem leftExp, NodeFunctionPtr rightExp)
{
    return leftExp.getNodeFunction() % std::move(rightExp);
}

NodeFunctionPtr POWER(FunctionItem leftExp, NodeFunctionPtr rightExp)
{
    return POWER(leftExp.getNodeFunction(), std::move(rightExp));
}

/// leftExp: node, rightExp: item
NodeFunctionPtr operator+(NodeFunctionPtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) + rightExp.getNodeFunction();
}

NodeFunctionPtr operator-(NodeFunctionPtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) - rightExp.getNodeFunction();
}

NodeFunctionPtr operator*(NodeFunctionPtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) * rightExp.getNodeFunction();
}

NodeFunctionPtr operator/(NodeFunctionPtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) / rightExp.getNodeFunction();
}

NodeFunctionPtr operator%(NodeFunctionPtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) % rightExp.getNodeFunction();
}

NodeFunctionPtr MOD(NodeFunctionPtr leftExp, FunctionItem rightExp)
{
    return std::move(leftExp) % rightExp.getNodeFunction();
}

NodeFunctionPtr POWER(NodeFunctionPtr leftExp, FunctionItem rightExp)
{
    return POWER(std::move(leftExp), rightExp.getNodeFunction());
}

/// leftExp: item, rightWxp: item
NodeFunctionPtr operator+(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getNodeFunction() + rightExp.getNodeFunction();
}

NodeFunctionPtr operator-(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getNodeFunction() - rightExp.getNodeFunction();
}

NodeFunctionPtr operator*(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getNodeFunction() * rightExp.getNodeFunction();
}

NodeFunctionPtr operator/(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getNodeFunction() / rightExp.getNodeFunction();
}

NodeFunctionPtr operator%(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getNodeFunction() % rightExp.getNodeFunction();
}

NodeFunctionPtr MOD(FunctionItem leftExp, FunctionItem rightExp)
{
    return leftExp.getNodeFunction() % rightExp.getNodeFunction();
}

NodeFunctionPtr POWER(FunctionItem leftExp, FunctionItem rightExp)
{
    return POWER(leftExp.getNodeFunction(), rightExp.getNodeFunction());
}

/// calls of Unary operators with FunctionItem
NodeFunctionPtr ABS(FunctionItem exp)
{
    return ABS(exp.getNodeFunction());
}

NodeFunctionPtr SQRT(FunctionItem exp)
{
    return SQRT(exp.getNodeFunction());
}

NodeFunctionPtr EXP(FunctionItem exp)
{
    return EXP(exp.getNodeFunction());
}

NodeFunctionPtr LN(FunctionItem exp)
{
    return LN(exp.getNodeFunction());
}

NodeFunctionPtr LOG2(FunctionItem exp)
{
    return LOG2(exp.getNodeFunction());
}

NodeFunctionPtr LOG10(FunctionItem exp)
{
    return LOG10(exp.getNodeFunction());
}

NodeFunctionPtr SIN(FunctionItem exp)
{
    return SIN(exp.getNodeFunction());
}

NodeFunctionPtr COS(FunctionItem exp)
{
    return COS(exp.getNodeFunction());
}

NodeFunctionPtr RADIANS(FunctionItem exp)
{
    return RADIANS(exp.getNodeFunction());
}

NodeFunctionPtr ROUND(FunctionItem exp)
{
    return ROUND(exp.getNodeFunction());
}

NodeFunctionPtr CEIL(FunctionItem exp)
{
    return CEIL(exp.getNodeFunction());
}

NodeFunctionPtr FLOOR(FunctionItem exp)
{
    return FLOOR(exp.getNodeFunction());
}

NodeFunctionPtr operator++(FunctionItem exp)
{
    return ++exp.getNodeFunction();
}

NodeFunctionPtr operator++(FunctionItem exp, int)
{
    return exp.getNodeFunction()++;
}

NodeFunctionPtr operator--(FunctionItem exp)
{
    return --exp.getNodeFunction();
}

NodeFunctionPtr operator--(FunctionItem exp, int)
{
    return exp.getNodeFunction()--;
}

}
