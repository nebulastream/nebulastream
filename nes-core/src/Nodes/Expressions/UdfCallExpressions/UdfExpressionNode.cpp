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
#include "Nodes/Expressions/FieldAccessExpressionNode.hpp"
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/UdfCallExpressions/UdfArgumentsNode.hpp>
#include <Nodes/Expressions/UdfCallExpressions/UdfExpressionNode.hpp>
#include <utility>

namespace NES::Experimental {

UdfExpressionNode::UdfExpressionNode(UdfExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getUdfName()->copy());
    addChildWithEqual(getFunctionArguments()->copy());
}

ExpressionNodePtr UdfExpressionNode::copy() {
    return std::make_shared<UdfExpressionNode>(UdfExpressionNode(this));
}

template<typename ConstantValueExpressionNodePtr>
ExpressionNodePtr UdfExpressionNode::create(const ConstantValueExpressionNodePtr udfName) {
    auto udfExpressionNode = std::make_shared<UdfExpressionNode>();
    udfExpressionNode->setChildren(udfName, nullptr);
    return udfExpressionNode;
}

template<typename ConstantValueExpressionNodePtr, typename... Args>
ExpressionNodePtr UdfExpressionNode::create(const ConstantValueExpressionNodePtr udfName,
                                            Args... functionArguments) {
    auto udfExpressionNode = std::make_shared<UdfExpressionNode>();
    std::vector<FieldAccessExpressionNodePtr> arguments({functionArguments...});
    udfExpressionNode->setChildren(udfName, arguments);
    return udfExpressionNode;
}

void UdfExpressionNode::setChildren(const ExpressionNodePtr& udfName, std::vector<ExpressionNodePtr> functionArguments) {
    addChild(udfName);
    auto tmp = UdfArgumentsNode::create(std::move(functionArguments));
    addChild(tmp);
}

void UdfExpressionNode::inferStamp(SchemaPtr schema) {
    auto left = getUdfName();
    auto right = getFunctionArguments();
    left->inferStamp(schema);
    right->inferStamp(schema);

    if (!left->getStamp()->isCharArray() || !right->getStamp()->isArray()) {
        throw std::logic_error(
            "UdfExpressionNode: Error during stamp inference. Types need to be Text and Array but Left was:"
            + left->getStamp()->toString() + " Right was: " + right->getStamp()->toString());
    }
    //TODO: Figure out return type
    stamp = DataTypeFactory::createInt32();
}

std::string UdfExpressionNode::toString() const {
    std::stringstream ss;
    ss << "CALL(" << children[0]->toString() << "," << children[1]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr UdfExpressionNode::getUdfName() {
    return children[0]->as<NES::ConstantValueExpressionNode>();
}

ExpressionNodePtr UdfExpressionNode::getFunctionArguments() {
    return children[1]->as<NES::ConstantValueExpressionNode>();
}


}// namespace NES::Experimental
#endif