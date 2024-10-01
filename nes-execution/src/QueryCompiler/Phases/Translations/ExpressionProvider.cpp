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

#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/DivExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterEqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessEqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/NegateExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/OrExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
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
    if (auto andNode = NES::Util::as_if<AndExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(andNode->getLeft());
        auto rightNautilusExpression = lowerExpression(andNode->getRight());
        return std::make_shared<AndExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto orNode = NES::Util::as_if<OrExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(orNode->getLeft());
        auto rightNautilusExpression = lowerExpression(orNode->getRight());
        return std::make_shared<OrExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto lessNode = NES::Util::as_if<LessExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(lessNode->getLeft());
        auto rightNautilusExpression = lowerExpression(lessNode->getRight());
        return std::make_shared<LessThanExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto equalsNode = NES::Util::as_if<EqualsExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(equalsNode->getLeft());
        auto rightNautilusExpression = lowerExpression(equalsNode->getRight());
        return std::make_shared<EqualsExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto greaterNode = NES::Util::as_if<GreaterExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(greaterNode->getLeft());
        auto rightNautilusExpression = lowerExpression(greaterNode->getRight());
        return std::make_shared<GreaterThanExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto greaterEqualsNode = NES::Util::as_if<GreaterEqualsExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(greaterEqualsNode->getLeft());
        auto rightNautilusExpression = lowerExpression(greaterEqualsNode->getRight());
        return std::make_shared<GreaterEqualsExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto lessEqualsNode = NES::Util::as_if<LessEqualsExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(lessEqualsNode->getLeft());
        auto rightNautilusExpression = lowerExpression(lessEqualsNode->getRight());
        return std::make_shared<LessEqualsExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto negateNode = NES::Util::as_if<NegateExpressionNode>(expressionNode))
    {
        auto child = lowerExpression(NES::Util::as<ExpressionNode>(negateNode->getChildren()[0]));
        return std::make_shared<NegateExpression>(child);
    }
    else if (auto mulNode = NES::Util::as_if<MulExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(mulNode->getLeft());
        auto rightNautilusExpression = lowerExpression(mulNode->getRight());
        return std::make_shared<MulExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto addNode = NES::Util::as_if<AddExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(addNode->getLeft());
        auto rightNautilusExpression = lowerExpression(addNode->getRight());
        return std::make_shared<AddExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto subNode = NES::Util::as_if<SubExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(subNode->getLeft());
        auto rightNautilusExpression = lowerExpression(subNode->getRight());
        return std::make_shared<SubExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto divNode = NES::Util::as_if<DivExpressionNode>(expressionNode))
    {
        auto leftNautilusExpression = lowerExpression(divNode->getLeft());
        auto rightNautilusExpression = lowerExpression(divNode->getRight());
        return std::make_shared<DivExpression>(leftNautilusExpression, rightNautilusExpression);
    }
    else if (auto functionExpression = NES::Util::as_if<FunctionExpression>(expressionNode))
    {
        return lowerFunctionExpression(functionExpression);
    }
    else if (auto constantValue = NES::Util::as_if<ConstantValueExpressionNode>(expressionNode))
    {
        return lowerConstantExpression(constantValue);
    }
    else if (auto fieldAccess = NES::Util::as_if<FieldAccessExpressionNode>(expressionNode))
    {
        return std::make_shared<ReadFieldExpression>(fieldAccess->getFieldName());
    }
    else
    {
        throw UnknownExpressionType();
    }
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
    auto functionProvider = ExecutableFunctionRegistry::instance().create(expressionNode->getFunctionName(), arguments);
    return functionProvider;
}
} /// namespace NES::QueryCompilation
