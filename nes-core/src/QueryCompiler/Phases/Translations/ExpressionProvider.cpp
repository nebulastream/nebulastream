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

#include <Common/DataTypes/DataType.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/DivExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ConstantInteger32Expression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Execution/Expressions/Functions/FactorialExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterEqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessEqualsExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/NegateExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/OrExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/Functions/FunctionExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <QueryCompiler/Phases/Translations/ExpressionProvider.hpp>
namespace NES::QueryCompilation {

std::shared_ptr<Runtime::Execution::Expressions::Expression>
ExpressionProvider::lowerExpression(const ExpressionNodePtr& expressionNode) {
    if (auto andNode = expressionNode->as_if<AndExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(andNode->getLeft());
        auto rightNautilusExpression = lowerExpression(andNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::AndExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto orNode = expressionNode->as_if<OrExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(orNode->getLeft());
        auto rightNautilusExpression = lowerExpression(orNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::OrExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto lessNode = expressionNode->as_if<LessExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(lessNode->getLeft());
        auto rightNautilusExpression = lowerExpression(lessNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::LessThanExpression>(leftNautilusExpression,
                                                                                     rightNautilusExpression);
    } else if (auto equalsNode = expressionNode->as_if<EqualsExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(equalsNode->getLeft());
        auto rightNautilusExpression = lowerExpression(equalsNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::EqualsExpression>(leftNautilusExpression,
                                                                                   rightNautilusExpression);
    } else if (auto greaterNode = expressionNode->as_if<GreaterExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(greaterNode->getLeft());
        auto rightNautilusExpression = lowerExpression(greaterNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::GreaterThanExpression>(leftNautilusExpression,
                                                                                        rightNautilusExpression);
    } else if (auto greaterEqualsNode = expressionNode->as_if<GreaterEqualsExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(greaterEqualsNode->getLeft());
        auto rightNautilusExpression = lowerExpression(greaterEqualsNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::GreaterEqualsExpression>(leftNautilusExpression,
                                                                                          rightNautilusExpression);
    } else if (auto lessEqualsNode = expressionNode->as_if<LessEqualsExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(lessEqualsNode->getLeft());
        auto rightNautilusExpression = lowerExpression(lessEqualsNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::LessEqualsExpression>(leftNautilusExpression,
                                                                                       rightNautilusExpression);
    } else if (auto negateNode = expressionNode->as_if<NegateExpressionNode>()) {
        auto child = lowerExpression(negateNode->getChildren()[0]->as<ExpressionNode>());
        return std::make_shared<Runtime::Execution::Expressions::NegateExpression>(child);
    } else if (auto mulNode = expressionNode->as_if<MulExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(mulNode->getLeft());
        auto rightNautilusExpression = lowerExpression(mulNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::MulExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto addNode = expressionNode->as_if<AddExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(addNode->getLeft());
        auto rightNautilusExpression = lowerExpression(addNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::AddExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto subNode = expressionNode->as_if<SubExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(subNode->getLeft());
        auto rightNautilusExpression = lowerExpression(subNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::SubExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto divNode = expressionNode->as_if<DivExpressionNode>()) {
        auto leftNautilusExpression = lowerExpression(divNode->getLeft());
        auto rightNautilusExpression = lowerExpression(divNode->getRight());
        return std::make_shared<Runtime::Execution::Expressions::DivExpression>(leftNautilusExpression, rightNautilusExpression);
    } else if (auto functionExpression = expressionNode->as_if<FunctionExpression>()) {
        return lowerFunctionExpression(functionExpression);
    } else if (auto constantValue = expressionNode->as_if<ConstantValueExpressionNode>()) {
        auto value = constantValue->getConstantValue();
        if (constantValue->getStamp()->isInteger()) {
            auto integerValue = std::stoi(value->as<BasicValue>()->value);
            return std::make_shared<Runtime::Execution::Expressions::ConstantInteger32Expression>(integerValue);
        } else {
            NES_NOT_IMPLEMENTED();
        }
    } else if (auto fieldAccess = expressionNode->as_if<FieldAccessExpressionNode>()) {
        return std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(fieldAccess->getFieldName());
    }
    NES_NOT_IMPLEMENTED();
}

std::shared_ptr<Runtime::Execution::Expressions::Expression>
ExpressionProvider::lowerFunctionExpression(const std::shared_ptr<FunctionExpression>& expressionNode) {
    std::vector<std::shared_ptr<Runtime::Execution::Expressions::Expression>> arguments;
    for (const auto& arg : expressionNode->getArguments()) {
        arguments.emplace_back(lowerExpression(arg));
    }
    auto functionProvider =
        Runtime::Execution::Expressions::ExecutableFunctionRegistry::createPlugin(expressionNode->getFunctionName());
    return functionProvider->create(arguments);
}
}// namespace NES::QueryCompilation