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
#include <Functions/ArithmeticalFunctions/NodeFunctionAbs.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionAdd.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionCeil.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionExp.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionFloor.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMod.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMul.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionPow.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionRound.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionSqrt.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionSub.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

///NodeFunction calls of binary operators with two FunctionItems
NodeFunctionPtr operator+(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionAdd::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator-(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionSub::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator*(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionMul::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator/(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionDiv::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr operator%(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionMod::create(std::move(functionLeft), std::move(functionRight));
}

NodeFunctionPtr MOD(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return std::move(functionLeft) % std::move(functionRight);
}

NodeFunctionPtr POWER(NodeFunctionPtr functionLeft, NodeFunctionPtr functionRight)
{
    return NodeFunctionPow::create(functionLeft, functionRight);
}

NodeFunctionPtr ABS(const NodeFunctionPtr& exp)
{
    return NodeFunctionAbs::create(exp);
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

NodeFunctionPtr operator++(NodeFunctionPtr functionLeft)
{
    return std::move(functionLeft) + NodeFunctionConstantValue::create(DataTypeFactory::createUInt16(), "1");
}

NodeFunctionPtr operator--(NodeFunctionPtr functionLeft)
{
    return std::move(functionLeft) - NodeFunctionConstantValue::create(DataTypeFactory::createUInt16(), "1");
}

NodeFunctionPtr operator++(NodeFunctionPtr functionLeft, int)
{
    return std::move(functionLeft) + NodeFunctionConstantValue::create(DataTypeFactory::createUInt16(), "1");
}

NodeFunctionPtr operator--(NodeFunctionPtr functionLeft, int)
{
    return std::move(functionLeft) - NodeFunctionConstantValue::create(DataTypeFactory::createUInt16(), "1");
}

/// calls of Binary operators with one or two FunctionItems
/// functionLeft: item, functionRight: node
NodeFunctionPtr operator+(FunctionItem functionLeft, NodeFunctionPtr functionRight)
{
    return functionLeft.getNodeFunction() + std::move(functionRight);
}

NodeFunctionPtr operator-(FunctionItem functionLeft, NodeFunctionPtr functionRight)
{
    return functionLeft.getNodeFunction() - std::move(functionRight);
}

NodeFunctionPtr operator*(FunctionItem functionLeft, NodeFunctionPtr functionRight)
{
    return functionLeft.getNodeFunction() * std::move(functionRight);
}

NodeFunctionPtr operator/(FunctionItem functionLeft, NodeFunctionPtr functionRight)
{
    return functionLeft.getNodeFunction() / std::move(functionRight);
}

NodeFunctionPtr operator%(FunctionItem functionLeft, NodeFunctionPtr functionRight)
{
    return functionLeft.getNodeFunction() % std::move(functionRight);
}

NodeFunctionPtr MOD(FunctionItem functionLeft, NodeFunctionPtr functionRight)
{
    return functionLeft.getNodeFunction() % std::move(functionRight);
}

NodeFunctionPtr POWER(FunctionItem functionLeft, NodeFunctionPtr functionRight)
{
    return POWER(functionLeft.getNodeFunction(), std::move(functionRight));
}

/// functionLeft: node, functionRight: item
NodeFunctionPtr operator+(NodeFunctionPtr functionLeft, FunctionItem functionRight)
{
    return std::move(functionLeft) + functionRight.getNodeFunction();
}

NodeFunctionPtr operator-(NodeFunctionPtr functionLeft, FunctionItem functionRight)
{
    return std::move(functionLeft) - functionRight.getNodeFunction();
}

NodeFunctionPtr operator*(NodeFunctionPtr functionLeft, FunctionItem functionRight)
{
    return std::move(functionLeft) * functionRight.getNodeFunction();
}

NodeFunctionPtr operator/(NodeFunctionPtr functionLeft, FunctionItem functionRight)
{
    return std::move(functionLeft) / functionRight.getNodeFunction();
}

NodeFunctionPtr operator%(NodeFunctionPtr functionLeft, FunctionItem functionRight)
{
    return std::move(functionLeft) % functionRight.getNodeFunction();
}

NodeFunctionPtr MOD(NodeFunctionPtr functionLeft, FunctionItem functionRight)
{
    return std::move(functionLeft) % functionRight.getNodeFunction();
}

NodeFunctionPtr POWER(NodeFunctionPtr functionLeft, FunctionItem functionRight)
{
    return POWER(std::move(functionLeft), functionRight.getNodeFunction());
}

/// functionLeft: item, rightWxp: item
NodeFunctionPtr operator+(FunctionItem functionLeft, FunctionItem functionRight)
{
    return functionLeft.getNodeFunction() + functionRight.getNodeFunction();
}

NodeFunctionPtr operator-(FunctionItem functionLeft, FunctionItem functionRight)
{
    return functionLeft.getNodeFunction() - functionRight.getNodeFunction();
}

NodeFunctionPtr operator*(FunctionItem functionLeft, FunctionItem functionRight)
{
    return functionLeft.getNodeFunction() * functionRight.getNodeFunction();
}

NodeFunctionPtr operator/(FunctionItem functionLeft, FunctionItem functionRight)
{
    return functionLeft.getNodeFunction() / functionRight.getNodeFunction();
}

NodeFunctionPtr operator%(FunctionItem functionLeft, FunctionItem functionRight)
{
    return functionLeft.getNodeFunction() % functionRight.getNodeFunction();
}

NodeFunctionPtr MOD(FunctionItem functionLeft, FunctionItem functionRight)
{
    return functionLeft.getNodeFunction() % functionRight.getNodeFunction();
}

NodeFunctionPtr POWER(FunctionItem functionLeft, FunctionItem functionRight)
{
    return POWER(functionLeft.getNodeFunction(), functionRight.getNodeFunction());
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
