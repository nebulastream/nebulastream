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

#ifdef PYTHON_UDF_ENABLED
#include <Nodes/Expressions/UdfCallExpressions/UdfArgumentsNode.hpp>
#include <utility>

namespace NES::Experimental {

ExpressionNodePtr UdfArgumentsNode::create(std::vector<ExpressionNodePtr> functionArgs) {
    auto udfArgumentsNode = std::make_shared<UdfArgumentsNode>();
    setFunctionArguments(std::move(functionArgs));
    return udfArgumentsNode;
}

std::string UdfArgumentsNode::toString() const {
    std::stringstream ss;
    ss << "Arguments(";
    for (const auto& argument : functionArguments) {
        ss << argument->getStamp() << ",";
    }
    ss << ")";
    return ss.str();
}

std::vector<ExpressionNodePtr> UdfArgumentsNode::getFunctionArguments() {
    return functionArguments;
}

ExpressionNodePtr UdfArgumentsNode::copy() {
    return std::make_shared<UdfArgumentsNode>(UdfArgumentsNode(this));
}
void UdfArgumentsNode::setFunctionArguments(std::vector<ExpressionNodePtr> functionArgs) {
    functionArguments = std::move(functionArgs);
}

}// namespace NES::Experimental
#endif