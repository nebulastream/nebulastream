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

#include <Catalogs/UDF/UdfDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Exceptions/UdfException.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/UdfCallExpressions/UdfCallExpressionNode.hpp>
#include <Optimizer/Phases/TypeInferencePhaseContext.hpp>
#include <utility>

namespace NES {

UdfCallExpressionNode::UdfCallExpressionNode(UdfCallExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getUdfNameNode()->copy());
}

UdfCallExpressionNode::UdfCallExpressionNode(const ConstantValueExpressionNodePtr& udfName,
                                             std::vector<ExpressionNodePtr> functionArguments)
    : ExpressionNode(DataTypeFactory::createUndefined()) {
    setChildren(udfName, std::move(functionArguments));
}

ExpressionNodePtr UdfCallExpressionNode::create(const ConstantValueExpressionNodePtr& udfName,
                                                const std::vector<ExpressionNodePtr>& functionArguments) {
    auto udfExpressionNode = std::make_shared<UdfCallExpressionNode>(udfName, functionArguments);
    return udfExpressionNode;
}

void UdfCallExpressionNode::setChildren(const ConstantValueExpressionNodePtr& udfName,
                                        std::vector<ExpressionNodePtr> functionArguments) {
    this->udfName = udfName;
    this->functionArguments = std::move(functionArguments);
}

void UdfCallExpressionNode::inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext, SchemaPtr schema) {
    auto left = getUdfNameNode();
    left->inferStamp(typeInferencePhaseContext, schema);
    if (!left->getStamp()->isCharArray()) {
        throw UdfException("UdfCallExpressionNode: Error during stamp inference. Function name needs to be Text but was:"
                           + left->getStamp()->toString());
    }
    auto udfDescriptorPtr = typeInferencePhaseContext.getUdfCatalog()->getUdfDescriptor(getUdfName());
    setUdfDescriptorPtr(Catalogs::UDF::UdfDescriptor::as<Catalogs::UDF::UdfDescriptor>(udfDescriptorPtr));
    if (udfDescriptor == nullptr) {
        throw UdfException("UdfCallExpressionNode: Error during stamp/return type inference. No UdfDescriptor was set");
    }
    stamp = udfDescriptor->getReturnType();
}

bool UdfCallExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<UdfCallExpressionNode>()) {
        auto otherUdfCallNode = rhs->as<UdfCallExpressionNode>();
        auto otherFunctionArguments = otherUdfCallNode->getFunctionArguments();
        if (otherFunctionArguments.size() != functionArguments.size()) {
            return false;
        }
        for (std::size_t i = 0; i < functionArguments.size(); ++i) {
            if (!otherFunctionArguments[i]->equal(functionArguments[i])) {
                return false;
            }
        }
        return getUdfNameNode()->equal(otherUdfCallNode->getUdfNameNode());
    }
    return false;
}

std::string UdfCallExpressionNode::toString() const {
    std::stringstream ss;
    ss << "CALL(" << getUdfName() << ",";
    ss << "Arguments(";
    for (const auto& argument : functionArguments) {
        ss << argument->toString() << ",";
    }
    ss << ")";
    return ss.str();
}

ExpressionNodePtr UdfCallExpressionNode::copy() { return std::make_shared<UdfCallExpressionNode>(UdfCallExpressionNode(this)); }

ExpressionNodePtr UdfCallExpressionNode::getUdfNameNode() const { return udfName; }

std::vector<ExpressionNodePtr> UdfCallExpressionNode::getFunctionArguments() { return functionArguments; }

void UdfCallExpressionNode::setUdfDescriptorPtr(const Catalogs::UDF::UdfDescriptorPtr& udfDescriptor) {
    this->udfDescriptor = udfDescriptor;
}

const std::string& UdfCallExpressionNode::getUdfName() const {
    auto constantValue = std::dynamic_pointer_cast<BasicValue>(udfName->getConstantValue());
    return constantValue->value;
}

}// namespace NES