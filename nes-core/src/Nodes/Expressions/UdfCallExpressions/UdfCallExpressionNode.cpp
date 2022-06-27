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
#include <Catalogs/UDF/PythonUdfDescriptor.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/UdfCallExpressions/UdfArgumentsNode.hpp>
#include <Nodes/Expressions/UdfCallExpressions/UdfCallExpressionNode.hpp>
#include <utility>

namespace NES::Experimental {

UdfCallExpressionNode::UdfCallExpressionNode(UdfCallExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getUdfName()->copy());
    addChildWithEqual(getFunctionArgumentsNode()->copy());
}

template<typename... Args>
ExpressionNodePtr UdfCallExpressionNode::create(const ConstantValueExpressionNodePtr& udfName,
                                            Args... functionArguments) {
    auto udfExpressionNode = std::make_shared<UdfCallExpressionNode>();
    std::vector<ExpressionNodePtr> arguments({functionArguments...});
    udfExpressionNode->setChildren(udfName, arguments);
    return udfExpressionNode;
}

void UdfCallExpressionNode::setChildren(const ExpressionNodePtr& udfName, std::vector<ExpressionNodePtr> functionArguments) {
    addChild(udfName);
    auto functionArgsNode = UdfArgumentsNode::create(std::move(functionArguments));
    addChild(functionArgsNode);
}

void UdfCallExpressionNode::inferStamp(SchemaPtr schema) {
    auto left = getUdfName();
    left->inferStamp(schema);

    if (!left->getStamp()->isCharArray()) {
        throw std::logic_error(
            "UdfCallExpressionNode: Error during stamp inference. Type needs to be Text but Left was:"
            + left->getStamp()->toString());
    }
    if (pythonUdfDescriptorPtr != nullptr)
        stamp = pythonUdfDescriptorPtr->getReturnType();
    else
        // If no udfDescriptor is set, we can't infer the stamp (for now)
        stamp = DataTypeFactory::createUndefined();
}

std::string UdfCallExpressionNode::toString() const {
    std::stringstream ss;
    ss << "CALL(" << children[0]->toString() << "," << children[1]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr UdfCallExpressionNode::copy() {
    return std::make_shared<UdfCallExpressionNode>(UdfCallExpressionNode(this));
}

ExpressionNodePtr UdfCallExpressionNode::getUdfName() {
    return children[0]->as<NES::ConstantValueExpressionNode>();
}

ExpressionNodePtr UdfCallExpressionNode::getFunctionArgumentsNode() {
    return children[1]->as<UdfArgumentsNode>();
}
void UdfCallExpressionNode::setPythonUdfDescriptorPtr(const Catalogs::PythonUdfDescriptorPtr& pyUdfDescriptor) {
    pythonUdfDescriptorPtr = pyUdfDescriptor;
}

}// namespace NES::Experimental
#endif