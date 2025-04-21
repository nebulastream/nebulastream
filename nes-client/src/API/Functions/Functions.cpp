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
#include <string>
#include <utility>
#include <vector>
#include <API/Functions/ArithmeticalFunctions.hpp>
#include <API/Functions/Functions.hpp>
#include <Functions/ConstantExpression.hpp>
#include <Functions/FieldAccessExpression.hpp>
#include <Functions/FieldRenameExpression.hpp>
#include <Functions/FunctionExpression.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <WellKnownDataTypes.hpp>

namespace NES
{

/// TODO #391: Use std::from_chars to check value for data type validity
FunctionItem::FunctionItem(int8_t value)
    : FunctionItem(
          make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Integer))
{
}

FunctionItem::FunctionItem(uint8_t value)
    : FunctionItem(
          make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Integer))
{
}

FunctionItem::FunctionItem(int16_t value)
    : FunctionItem(
          make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Integer))
{
}

FunctionItem::FunctionItem(uint16_t value)
    : FunctionItem(
          make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Integer))
{
}

FunctionItem::FunctionItem(int32_t value)
    : FunctionItem(
          make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Integer))
{
}

FunctionItem::FunctionItem(uint32_t value)
    : FunctionItem(
          make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Integer))
{
}

FunctionItem::FunctionItem(int64_t value)
    : FunctionItem(
          make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Integer))
{
}

FunctionItem::FunctionItem(uint64_t value)
    : FunctionItem(
          make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Integer))
{
}

FunctionItem::FunctionItem(float value)
    : FunctionItem(
          make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Integer))
{
    throw NotImplemented("Floats Are not Implemented");
}

FunctionItem::FunctionItem(double value)
    : FunctionItem(
          make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Integer))
{
    throw NotImplemented("Floats Are not Implemented");
}

FunctionItem::FunctionItem(bool value)
    : FunctionItem(make_expression<FunctionExpression>({make_expression<ConstantExpression>({}, std::to_string(value))}, WellKnown::Bool))
{
}
FunctionItem::FunctionItem(ExpressionValue exp) : function(std::move(exp))
{
}

FunctionItem FunctionItem::as(std::string)
{
    throw NotImplemented("Not implemented");
}

ExpressionValue FunctionItem::operator=(const FunctionItem&) const
{
    throw NotImplemented("Not implemented");
}

ExpressionValue FunctionItem::operator=(ExpressionValue) const
{
    throw NotImplemented("Not implemented");
}

FunctionItem Attribute(std::string fieldName)
{
    return {make_expression<FieldAccessExpression>(Schema::Identifier(std::move(fieldName)))};
}


ExpressionValue FunctionItem::getNodeFunction() const
{
    return function;
}

FunctionItem::operator ExpressionValue()
{
    return function;
}

}
