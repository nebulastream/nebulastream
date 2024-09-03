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

#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/Functions/FunctionExpressionNode.hpp>
#include <Expressions/Functions/LogicalFunctionRegistry.hpp>
#include <Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <QueryCompiler/Phases/Translations/ExpressionProvider.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/ValueTypes/BasicValue.hpp>

namespace NES::QueryCompilation
{
using namespace Runtime::Execution::Expressions;

std::shared_ptr<Expression> ExpressionProvider::lowerExpression(const ExpressionNodePtr& expressionNode)
{
    NES_INFO("Lower Expression {}", expressionNode->toString())
    if (auto constantValue = expressionNode->as_if<ConstantValueExpressionNode>())
    {
        return lowerConstantExpression(constantValue);
    }
    if (auto fieldAccess = NES::Util::as_if<FieldAccessExpressionNode>(expressionNode))
    {
        return std::make_shared<ReadFieldExpression>(fieldAccess->getFieldName());
    }
    if (auto fieldWrite = expressionNode->as_if<FieldAssignmentExpressionNode>())
    {
        return std::make_shared<WriteFieldExpression>(fieldWrite->getField()->getFieldName(), lowerExpression(fieldWrite->getAssignment()));
    }
    else if (auto functionExpression = expressionNode->as_if<FunctionExpression>())
    {
        return lowerFunctionExpression(functionExpression);
    }

    throw UnknownExpressionType();
}

ExpressionPtr ExpressionProvider::lowerConstantExpression(const std::shared_ptr<ConstantValueExpressionNode>& constantExpression)
{
    auto value = constantExpression->getConstantValue();
    auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(constantExpression->getStamp());
    if (physicalType->isBasicType())
    {
        auto stringValue = std::dynamic_pointer_cast<BasicValue>(value)->value;
        auto basicType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::UINT_8: {
                auto intValue = (uint8_t)std::stoul(stringValue);
                return std::make_shared<ConstantUInt8ValueExpression>(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                auto intValue = (uint16_t)std::stoul(stringValue);
                return std::make_shared<ConstantUInt16ValueExpression>(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                auto intValue = (uint32_t)std::stoul(stringValue);
                return std::make_shared<ConstantUInt32ValueExpression>(intValue);
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                auto intValue = (uint64_t)std::stoull(stringValue);
                return std::make_shared<ConstantUInt64ValueExpression>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_8: {
                auto intValue = (int8_t)std::stoi(stringValue);
                return std::make_shared<ConstantInt8ValueExpression>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_16: {
                auto intValue = (int16_t)std::stoi(stringValue);
                return std::make_shared<ConstantInt16ValueExpression>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_32: {
                auto intValue = (int32_t)std::stoi(stringValue);
                return std::make_shared<ConstantInt32ValueExpression>(intValue);
            };
            case BasicPhysicalType::NativeType::INT_64: {
                auto intValue = (int64_t)std::stol(stringValue);
                return std::make_shared<ConstantInt64ValueExpression>(intValue);
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                auto floatValue = std::stof(stringValue);
                return std::make_shared<ConstantFloatValueExpression>(floatValue);
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                auto doubleValue = std::stod(stringValue);
                return std::make_shared<ConstantDoubleValueExpression>(doubleValue);
            };
            case BasicPhysicalType::NativeType::CHAR:
                break;
            case BasicPhysicalType::NativeType::BOOLEAN: {
                auto boolValue = (bool)std::stoi(stringValue) == 1;
                return std::make_shared<ConstantBooleanValueExpression>(boolValue);
            };
            default: {
                throw UnknownPhysicalType("the basic type is not supported");
            }
        }
    }
    throw UnknownPhysicalType("type must be a basic types");
}

std::shared_ptr<Expression> ExpressionProvider::lowerFunctionExpression(const std::shared_ptr<FunctionExpression>& expressionNode)
{
    std::vector<std::shared_ptr<Expression>> arguments;
    for (const auto& arg : expressionNode->getArguments())
    {
        arguments.emplace_back(lowerExpression(arg));
    }
    if (auto functionProvider = ExecutableFunctionRegistry::instance().create(expressionNode->getFunctionName(), arguments))
        return std::move(functionProvider.value());
    throw UnknownExpressionType(fmt::format("Expression plugin of type: {} not registered.", expressionNode->getFunctionName()));
}
}
