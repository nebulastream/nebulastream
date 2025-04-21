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
#include <Functions/ConstantExpression.hpp>
#include <Functions/FunctionExpression.hpp>
#include <Functions/WellKnownFunctions.hpp>

#include <WellKnownDataTypes.hpp>

namespace NES
{

///NodeFunction calls of binary operators with two FunctionItems
ExpressionValue operator+(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::Add);
}

ExpressionValue operator-(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::Sub);
}

ExpressionValue operator*(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::Mul);
}

ExpressionValue operator/(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::Div);
}

ExpressionValue operator%(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::Mod);
}

ExpressionValue MOD(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return std::move(functionLeft) % std::move(functionRight);
}

ExpressionValue POWER(ExpressionValue functionLeft, ExpressionValue functionRight)
{
    return make_expression<FunctionExpression>({functionLeft, functionRight}, WellKnown::Pow);
}

ExpressionValue ABS(ExpressionValue otherFunction)
{
    return make_expression<FunctionExpression>({otherFunction}, WellKnown::Abs);
}

ExpressionValue SQRT(ExpressionValue otherFunction)
{
    return make_expression<FunctionExpression>({otherFunction}, WellKnown::Sqrt);
}

ExpressionValue EXP(ExpressionValue otherFunction)
{
    return make_expression<FunctionExpression>({otherFunction}, WellKnown::Exp);
}

ExpressionValue ROUND(ExpressionValue otherFunction)
{
    return make_expression<FunctionExpression>({otherFunction}, WellKnown::Round);
}

ExpressionValue CEIL(ExpressionValue otherFunction)
{
    return make_expression<FunctionExpression>({otherFunction}, WellKnown::Ceil);
}

ExpressionValue FLOOR(ExpressionValue otherFunction)
{
    return make_expression<FunctionExpression>({otherFunction}, WellKnown::Floor);
}

ExpressionValue operator++(ExpressionValue otherFunction)
{
    return otherFunction + make_expression<FunctionExpression>({make_expression<ConstantExpression>("1")}, WellKnown::Integer);
}

ExpressionValue operator--(ExpressionValue otherFunction)
{
    return otherFunction - make_expression<FunctionExpression>({make_expression<ConstantExpression>("1")}, WellKnown::Integer);
}

const ExpressionValue operator++(ExpressionValue otherFunction, int)
{
    return otherFunction + make_expression<FunctionExpression>({make_expression<ConstantExpression>("1")}, WellKnown::Integer);
}

const ExpressionValue operator--(ExpressionValue otherFunction, int)
{
    return otherFunction - make_expression<FunctionExpression>({make_expression<ConstantExpression>("1")}, WellKnown::Integer);
}

/// calls of Binary operators with one or two FunctionItems
/// functionLeft: item, functionRight: node
ExpressionValue operator+(const FunctionItem& functionLeft, ExpressionValue functionRight)
{
    return functionLeft.getNodeFunction() + std::move(functionRight);
}

ExpressionValue operator-(const FunctionItem& functionLeft, ExpressionValue functionRight)
{
    return functionLeft.getNodeFunction() - std::move(functionRight);
}

ExpressionValue operator*(const FunctionItem& functionLeft, ExpressionValue functionRight)
{
    return functionLeft.getNodeFunction() * std::move(functionRight);
}

ExpressionValue operator/(const FunctionItem& functionLeft, ExpressionValue functionRight)
{
    return functionLeft.getNodeFunction() / std::move(functionRight);
}

ExpressionValue operator%(const FunctionItem& functionLeft, ExpressionValue functionRight)
{
    return functionLeft.getNodeFunction() % std::move(functionRight);
}

ExpressionValue MOD(const FunctionItem& functionLeft, ExpressionValue functionRight)
{
    return functionLeft.getNodeFunction() % std::move(functionRight);
}

ExpressionValue POWER(const FunctionItem& functionLeft, ExpressionValue functionRight)
{
    return POWER(functionLeft.getNodeFunction(), std::move(functionRight));
}

/// functionLeft: node, functionRight: item
ExpressionValue operator+(ExpressionValue functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) + functionRight.getNodeFunction();
}

ExpressionValue operator-(ExpressionValue functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) - functionRight.getNodeFunction();
}

ExpressionValue operator*(ExpressionValue functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) * functionRight.getNodeFunction();
}

ExpressionValue operator/(ExpressionValue functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) / functionRight.getNodeFunction();
}

ExpressionValue operator%(ExpressionValue functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) % functionRight.getNodeFunction();
}

ExpressionValue MOD(ExpressionValue functionLeft, const FunctionItem& functionRight)
{
    return std::move(functionLeft) % functionRight.getNodeFunction();
}

ExpressionValue POWER(ExpressionValue functionLeft, const FunctionItem& functionRight)
{
    return POWER(std::move(functionLeft), functionRight.getNodeFunction());
}

/// functionLeft: item, rightWxp: item
ExpressionValue operator+(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() + functionRight.getNodeFunction();
}

ExpressionValue operator-(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() - functionRight.getNodeFunction();
}

ExpressionValue operator*(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() * functionRight.getNodeFunction();
}

ExpressionValue operator/(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() / functionRight.getNodeFunction();
}

ExpressionValue operator%(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() % functionRight.getNodeFunction();
}

ExpressionValue MOD(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return functionLeft.getNodeFunction() % functionRight.getNodeFunction();
}

ExpressionValue POWER(const FunctionItem& functionLeft, const FunctionItem& functionRight)
{
    return POWER(functionLeft.getNodeFunction(), functionRight.getNodeFunction());
}

/// calls of Unary operators with FunctionItem
ExpressionValue ABS(const FunctionItem& otherFunction)
{
    return ABS(otherFunction.getNodeFunction());
}

ExpressionValue SQRT(const FunctionItem& otherFunction)
{
    return SQRT(otherFunction.getNodeFunction());
}

ExpressionValue EXP(const FunctionItem& otherFunction)
{
    return EXP(otherFunction.getNodeFunction());
}

ExpressionValue ROUND(const FunctionItem& otherFunction)
{
    return ROUND(otherFunction.getNodeFunction());
}

ExpressionValue CEIL(const FunctionItem& otherFunction)
{
    return CEIL(otherFunction.getNodeFunction());
}

ExpressionValue FLOOR(const FunctionItem& otherFunction)
{
    return FLOOR(otherFunction.getNodeFunction());
}

ExpressionValue operator++(const FunctionItem& otherFunction)
{
    return ++otherFunction.getNodeFunction();
}

const ExpressionValue operator++(const FunctionItem& otherFunction, int)
{
    return otherFunction.getNodeFunction()++;
}

ExpressionValue operator--(const FunctionItem& otherFunction)
{
    return --otherFunction.getNodeFunction();
}

const ExpressionValue operator--(const FunctionItem& otherFunction, int)
{
    return otherFunction.getNodeFunction()--;
}

}
