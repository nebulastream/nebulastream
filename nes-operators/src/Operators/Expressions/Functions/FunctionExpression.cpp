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
#include <Operators/Expressions/Functions/FunctionExpressionNode.hpp>
#include <Operators/Expressions/Functions/LogicalFunctionRegistry.hpp>
#include <utility>

namespace NES {

FunctionExpression::FunctionExpression(DataTypePtr stamp, std::string functionName, std::unique_ptr<LogicalFunction> function)
    : ExpressionNode(std::move(stamp)), functionName(std::move(functionName)), function(std::move(function)) {}

ExpressionNodePtr FunctionExpression::create(const DataTypePtr& stamp,
                                             const std::string& functionName,
                                             const std::vector<ExpressionNodePtr>& arguments) {
    auto function = LogicalFunctionRegistry::createPlugin(functionName);
    auto expression = std::make_shared<FunctionExpression>(stamp, functionName, std::move(function));
    for (const auto& arg : arguments) {
        expression->addChild(arg);
    }
    return expression;
}

void FunctionExpression::inferStamp( SchemaPtr schema) {
    std::vector<DataTypePtr> argumentTypes;
    for (const auto& input : getArguments()) {
        input->inferStamp( schema);
        argumentTypes.emplace_back(input->getStamp());
    }
    auto resultStamp = function->inferStamp(argumentTypes);
    setStamp(resultStamp);
}

std::string FunctionExpression::toString() const { return functionName; }

bool FunctionExpression::equal(const NodePtr& rhs) const {
    if (rhs->instanceOf<FunctionExpression>()) {
        auto otherAddNode = rhs->as<FunctionExpression>();
        return functionName == otherAddNode->functionName;
    }
    return false;
}

ExpressionNodePtr FunctionExpression::copy() { return FunctionExpression::create(stamp, functionName, getArguments()); }
const std::string& FunctionExpression::getFunctionName() const { return functionName; }
std::vector<ExpressionNodePtr> FunctionExpression::getArguments() const {
    std::vector<ExpressionNodePtr> arguments;
    for (const auto& child : getChildren()) {
        arguments.emplace_back(child->as<ExpressionNode>());
    }
    return arguments;
}

}// namespace NES