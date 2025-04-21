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
#include <ranges>
#include <vector>

#include <Functions/ConstantExpression.hpp>
#include <Functions/FieldAccessExpression.hpp>
#include <Functions/FieldAssignmentExpression.hpp>
#include <Functions/FunctionExpression.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <DataTypeSerializationFactory.hpp>
#include <SerializableFunction.pb.h>

namespace NES
{

void FunctionSerializationUtil::serializeExpression(const ExpressionValue& nodeFunction, SerializableFunction* serializedFunction)
{
    if (auto stamp = nodeFunction.getStamp())
    {
        DataTypeSerializationFactory::instance().serialize(*stamp, *serializedFunction->mutable_stamp());
    }

    for (const auto& child : nodeFunction.getChildren())
    {
        serializeExpression(child, serializedFunction->mutable_children()->Add());
    }

    if (auto constantExpr = nodeFunction.as<ConstantExpression>())
    {
        SerializableFunction_ConstantExpression constantExpression{};
        constantExpression.set_value(constantExpr->getConstantValue());
        serializedFunction->mutable_details()->PackFrom(constantExpression);
    }
    else if (auto functionExpression = nodeFunction.as<FunctionExpression>())
    {
        SerializableFunction_FunctionExpression function_expression{};
        function_expression.set_functionname(functionExpression->getFunctionName());
        serializedFunction->mutable_details()->PackFrom(function_expression);
    }
    else if (auto fieldAccessExpression = nodeFunction.as<FieldAccessExpression>())
    {
        SerializableFunction_FunctionFieldAccess fieldAccess{};
        SchemaSerializationUtil::serializeSchemaIdentifier(fieldAccessExpression->getFieldName(), *fieldAccess.mutable_field());
        serializedFunction->mutable_details()->PackFrom(fieldAccess);
    }
    else
    {
        throw std::runtime_error("FunctionSerializationUtil::serializeExpression: Unknown expression type.");
    }
}
ExpressionValue FunctionSerializationUtil::deserializeExpression(const SerializableFunction& serializedFunction)
{
    auto stamp = [&]() -> std::optional<DataType>
    {
        if (serializedFunction.has_stamp())
        {
            return DataTypeSerializationFactory::instance().deserialize(serializedFunction.stamp());
        }
        return {};
    }();

    auto children = serializedFunction.children()
        | std::views::transform([](const auto& child) { return FunctionSerializationUtil::deserializeExpression(child); })
        | std::ranges::to<std::vector>();

    if (serializedFunction.details().Is<SerializableFunction_ConstantExpression>())
    {
        SerializableFunction_ConstantExpression constantExpression{};
        serializedFunction.details().UnpackTo(&constantExpression);
        auto expression = std::make_shared<ConstantExpression>(constantExpression.value());
        expression->stamp = stamp;
        return {COW<Expression>(std::move(expression)), std::move(children)};
    }

    if (serializedFunction.details().Is<SerializableFunction_FunctionExpression>())
    {
        SerializableFunction_FunctionExpression functionExpression{};
        serializedFunction.details().UnpackTo(&functionExpression);
        auto expression = std::make_shared<FunctionExpression>(functionExpression.functionname());
        expression->stamp = stamp;
        return {COW<Expression>(std::move(expression)), std::move(children)};
    }

    if (serializedFunction.details().Is<SerializableFunction_FunctionFieldAccess>())
    {
        SerializableFunction_FunctionFieldAccess function_assignment{};
        serializedFunction.details().UnpackTo(&function_assignment);
        auto expression = std::make_shared<FieldAccessExpression>(
            SchemaSerializationUtil::deserializeSchemaIdentifier(function_assignment.field()));
        expression->stamp = stamp;
        return {COW<Expression>(std::move(expression)), std::move(children)};
    }

    throw std::runtime_error("FunctionSerializationUtil::deserializeExpression: Unknown expression type.");
}
}
