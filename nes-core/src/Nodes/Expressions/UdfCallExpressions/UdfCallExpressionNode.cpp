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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/UdfCallExpressions/UdfCallExpressionNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Catalogs/UDF/UdfDescriptor.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Exceptions/UdfException.hpp>
#include <utility>

namespace NES {

UdfCallExpressionNode::UdfCallExpressionNode() : ExpressionNode(DataTypeFactory::createBoolean()) {}

UdfCallExpressionNode::UdfCallExpressionNode(UdfCallExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getUdfNameNode()->copy());
}

ExpressionNodePtr UdfCallExpressionNode::create(const ConstantValueExpressionNodePtr& udfFunctionName,
                                                std::vector<ExpressionNodePtr> functionArgs) {
    auto udfExpressionNode = std::make_shared<UdfCallExpressionNode>();
    udfExpressionNode->setChildren(udfFunctionName, std::move(functionArgs));
    udfExpressionNode->setUdfName(udfFunctionName);
    return udfExpressionNode;
}

void UdfCallExpressionNode::setChildren(const ExpressionNodePtr& udfFunctionName, std::vector<ExpressionNodePtr> functionArgs) {
    addChild(udfFunctionName);
    functionArguments = std::move(functionArgs);
}

void UdfCallExpressionNode::inferStamp(const Optimizer::TypeInferencePhaseContext& ctx, SchemaPtr schema) {
    auto left = getUdfNameNode();
    left->inferStamp(ctx, schema);
    if (!left->getStamp()->isCharArray()) {
        throw UdfException(
            "UdfCallExpressionNode: Error during stamp inference. Function name needs to be Text but was:"
            + left->getStamp()->toString());
    }
    auto udfDescriptor = ctx.getUdfCatalog()->getUdfDescriptor(getUdfName());
    setUdfDescriptorPtr(Catalogs::UdfDescriptor::as<Catalogs::UdfDescriptor>(udfDescriptor));
    if (udfDescriptorPtr == nullptr) {
        throw UdfException("UdfCallExpressionNode: Error during stamp/return type inference. No UdfDescriptor was set");
    }
    else {
        stamp = udfDescriptorPtr->getReturnType();
    }
}

std::string UdfCallExpressionNode::toString() const {
    std::stringstream ss;
    ss << "CALL(" << children[0]->toString() << ",";
    ss << "Arguments(";
    for (const auto& argument : functionArguments) {
        ss << argument->getStamp() << ",";
    }
    ss << ")";
    return ss.str();
}

ExpressionNodePtr UdfCallExpressionNode::copy() {
    return std::make_shared<UdfCallExpressionNode>(UdfCallExpressionNode(this));
}

ExpressionNodePtr UdfCallExpressionNode::getUdfNameNode() {
    return children[0]->as<ConstantValueExpressionNode>();
}

std::vector<ExpressionNodePtr> UdfCallExpressionNode::getFunctionArguments() {
    return functionArguments;
}

void UdfCallExpressionNode::setUdfDescriptorPtr(const Catalogs::UdfDescriptorPtr& udfDescriptor) {
    udfDescriptorPtr = udfDescriptor;
}
void UdfCallExpressionNode::setUdfName(const ConstantValueExpressionNodePtr& udfFunctionName) {
    auto constantValue = std::dynamic_pointer_cast<BasicValue>(udfFunctionName->getConstantValue());
    udfName = constantValue->value;
}
const std::string& UdfCallExpressionNode::getUdfName() const {
    return udfName;
}

}// namespace NES::Experimental