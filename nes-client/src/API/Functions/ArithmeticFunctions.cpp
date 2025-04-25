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

#include <memory>
#include <utility>

#include <API/Functions/ArithmeticalFunctions.hpp>
#include <API/Functions/Functions.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
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
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionConstantValue.hpp>

namespace NES
{

///NodeFunction calls of binary operators with two FunctionItems
std::shared_ptr<NodeFunction>
operator+(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return NodeFunctionAdd::create(std::move(functionLeft), std::move(functionRight));
}

std::shared_ptr<NodeFunction>
operator-(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return NodeFunctionSub::create(std::move(functionLeft), std::move(functionRight));
}

std::shared_ptr<NodeFunction>
operator*(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return NodeFunctionMul::create(std::move(functionLeft), std::move(functionRight));
}

std::shared_ptr<NodeFunction>
operator/(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return NodeFunctionDiv::create(std::move(functionLeft), std::move(functionRight));
}

std::shared_ptr<NodeFunction>
operator%(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return NodeFunctionMod::create(std::move(functionLeft), std::move(functionRight));
}

std::shared_ptr<NodeFunction> MOD(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return std::move(functionLeft) % std::move(functionRight);
}

std::shared_ptr<NodeFunction> POWER(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return NodeFunctionPow::create(functionLeft, functionRight);
}

std::shared_ptr<NodeFunction> ABS(const std::shared_ptr<NodeFunction>& otherFunction)
{
    return NodeFunctionAbs::create(otherFunction);
}

std::shared_ptr<NodeFunction> SQRT(const std::shared_ptr<NodeFunction>& otherFunction)
{
    return NodeFunctionSqrt::create(otherFunction);
}

std::shared_ptr<NodeFunction> EXP(const std::shared_ptr<NodeFunction>& otherFunction)
{
    return NodeFunctionExp::create(otherFunction);
}

std::shared_ptr<NodeFunction> ROUND(const std::shared_ptr<NodeFunction>& otherFunction)
{
    return NodeFunctionRound::create(otherFunction);
}

std::shared_ptr<NodeFunction> CEIL(const std::shared_ptr<NodeFunction>& otherFunction)
{
    return NodeFunctionCeil::create(otherFunction);
}

std::shared_ptr<NodeFunction> FLOOR(const std::shared_ptr<NodeFunction>& otherFunction)
{
    return NodeFunctionFloor::create(otherFunction);
}

std::shared_ptr<NodeFunction> operator++(const std::shared_ptr<NodeFunction>& otherFunction)
{
    return otherFunction + NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(DataType::Type::UINT16), "1");
}

std::shared_ptr<NodeFunction> operator--(const std::shared_ptr<NodeFunction>& otherFunction)
{
    return otherFunction - NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(DataType::Type::UINT16), "1");
}

const std::shared_ptr<NodeFunction> operator++(const std::shared_ptr<NodeFunction>& otherFunction, int)
{
    return otherFunction + NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(DataType::Type::UINT16), "1");
}

const std::shared_ptr<NodeFunction> operator--(const std::shared_ptr<NodeFunction>& otherFunction, int)
{
    return otherFunction - NodeFunctionConstantValue::create(DataTypeProvider::provideDataType(DataType::Type::UINT16), "1");
}

/// calls of Binary operators with one or two FunctionItems
/// functionLeft: item, functionRight: node
std::shared_ptr<NodeFunction> operator+(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return functionLeft.getNodeFunction() + std::move(functionRight);
}

std::shared_ptr<NodeFunction> operator-(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return functionLeft.getNodeFunction() - std::move(functionRight);
}

std::shared_ptr<NodeFunction> operator*(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return functionLeft.getNodeFunction() * std::move(functionRight);
}

std::shared_ptr<NodeFunction> operator/(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return functionLeft.getNodeFunction() / std::move(functionRight);
}

std::shared_ptr<NodeFunction> operator%(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return functionLeft.getNodeFunction() % std::move(functionRight);
}

std::shared_ptr<NodeFunction> MOD(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return functionLeft.getNodeFunction() % std::move(functionRight);
}

std::shared_ptr<NodeFunction> POWER(const FunctionItem& functionLeft, const std::shared_ptr<NodeFunction>& functionRight)
{
    return POWER(functionLeft.getNodeFunction(), std::move(functionRight));
}

/// functionLeft: node, functionRight: item
std::shared_ptr<NodeFunction> operator+(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) + functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> operator-(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) - functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> operator*(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) * functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> operator/(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) / functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> operator%(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) % functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> MOD(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) % functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> POWER(const std::shared_ptr<NodeFunction>& functionLeft, const FunctionItem& functionRight)
{
    return POWER(std::move(functionLeft), functionRight.getNodeFunction());
}

/// functionLeft: item, rightWxp: item
std::shared_ptr<NodeFunction> operator+(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() + functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> operator-(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() - functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> operator*(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() * functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> operator/(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() / functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> operator%(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() % functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> MOD(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() % functionRight.getNodeFunction();
}

std::shared_ptr<NodeFunction> POWER(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return POWER(functionLeft.getNodeFunction(), functionRight.getNodeFunction());
}

/// calls of Unary operators with FunctionItem
std::shared_ptr<NodeFunction> ABS(const FunctionItem& otherFunction)
{
    return ABS(otherFunction.getNodeFunction());
}

std::shared_ptr<NodeFunction> SQRT(const FunctionItem& otherFunction)
{
    return SQRT(otherFunction.getNodeFunction());
}

std::shared_ptr<NodeFunction> EXP(const FunctionItem& otherFunction)
{
    return EXP(otherFunction.getNodeFunction());
}

std::shared_ptr<NodeFunction> ROUND(const FunctionItem& otherFunction)
{
    return ROUND(otherFunction.getNodeFunction());
}

std::shared_ptr<NodeFunction> CEIL(const FunctionItem& otherFunction)
{
    return CEIL(otherFunction.getNodeFunction());
}

std::shared_ptr<NodeFunction> FLOOR(const FunctionItem& otherFunction)
{
    return FLOOR(otherFunction.getNodeFunction());
}

std::shared_ptr<NodeFunction> operator++(const FunctionItem& otherFunction)
{
    return ++otherFunction.getNodeFunction();
}

const std::shared_ptr<NodeFunction> operator++(const FunctionItem& otherFunction, int)
{
    return otherFunction.getNodeFunction()++;
}

std::shared_ptr<NodeFunction> operator--(const FunctionItem& otherFunction)
{
    return --otherFunction.getNodeFunction();
}

const std::shared_ptr<NodeFunction> operator--(const FunctionItem& otherFunction, int)
{
    return otherFunction.getNodeFunction()--;
}

}
