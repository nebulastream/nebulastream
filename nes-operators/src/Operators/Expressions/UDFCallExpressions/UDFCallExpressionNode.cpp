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

#include <Catalogs/UDF/UDFDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Exceptions/UDFException.hpp>
#include <Operators/Expressions/ExpressionNode.hpp>
#include <Operators/Expressions/UDFCallExpressions/UDFCallExpressionNode.hpp>
#include <Optimizer/Phases/TypeInferencePhaseContext.hpp>
#include <sstream>
#include <utility>

namespace NES {

UDFCallExpressionNode::UDFCallExpressionNode(UDFCallExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getUDFNameNode()->copy());
}

UDFCallExpressionNode::UDFCallExpressionNode(const ConstantValueExpressionNodePtr& udfName,
                                             std::vector<ExpressionNodePtr> functionArguments)
    : ExpressionNode(DataTypeFactory::createUndefined()) {
    setChildren(udfName, std::move(functionArguments));
}

ExpressionNodePtr UDFCallExpressionNode::create(const ConstantValueExpressionNodePtr& udfName,
                                                const std::vector<ExpressionNodePtr>& functionArguments) {
    auto udfExpressionNode = std::make_shared<UDFCallExpressionNode>(udfName, functionArguments);
    return udfExpressionNode;
}

void UDFCallExpressionNode::setChildren(const ConstantValueExpressionNodePtr& udfName,
                                        std::vector<ExpressionNodePtr> functionArguments) {
    this->udfName = udfName;
    this->functionArguments = std::move(functionArguments);
}

void UDFCallExpressionNode::inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext, SchemaPtr schema) {
    auto left = getUDFNameNode();
    left->inferStamp(typeInferencePhaseContext, schema);
    if (!left->getStamp()->isCharArray()) {
        throw UDFException("UDFCallExpressionNode: Error during stamp inference. Function name needs to be Text but was:"
                           + left->getStamp()->toString());
    }
    auto UDFDescriptorPtr = typeInferencePhaseContext.getUDFCatalog()->getUDFDescriptor(getUDFName());
    setUDFDescriptorPtr(Catalogs::UDF::UDFDescriptor::as<Catalogs::UDF::UDFDescriptor>(UDFDescriptorPtr));
    if (UDFDescriptor == nullptr) {
        throw UDFException("UDFCallExpressionNode: Error during stamp/return type inference. No UDFDescriptor was set");
    }
    stamp = UDFDescriptor->getReturnType();
}

bool UDFCallExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<UDFCallExpressionNode>()) {
        auto otherUdfCallNode = rhs->as<UDFCallExpressionNode>();
        auto otherFunctionArguments = otherUdfCallNode->getFunctionArguments();
        if (otherFunctionArguments.size() != functionArguments.size()) {
            return false;
        }
        for (std::size_t i = 0; i < functionArguments.size(); ++i) {
            if (!otherFunctionArguments[i]->equal(functionArguments[i])) {
                return false;
            }
        }
        return getUDFNameNode()->equal(otherUdfCallNode->getUDFNameNode());
    }
    return false;
}

std::string UDFCallExpressionNode::toString() const {
    std::stringstream ss;
    ss << "CALL(" << getUDFName() << ",";
    ss << "Arguments(";
    for (const auto& argument : functionArguments) {
        ss << argument->toString() << ",";
    }
    ss << ")";
    return ss.str();
}

ExpressionNodePtr UDFCallExpressionNode::copy() {
    std::vector<ExpressionNodePtr> copyOfFunctionArguments;
    for (auto functionArgument : functionArguments) {
        copyOfFunctionArguments.push_back(functionArgument->copy());
    }
    return UDFCallExpressionNode::create(udfName->copy()->as<ConstantValueExpressionNode>(), copyOfFunctionArguments);
}

ExpressionNodePtr UDFCallExpressionNode::getUDFNameNode() const { return udfName; }

std::vector<ExpressionNodePtr> UDFCallExpressionNode::getFunctionArguments() { return functionArguments; }

void UDFCallExpressionNode::setUDFDescriptorPtr(const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor) {
    this->UDFDescriptor = udfDescriptor;
}

const std::string& UDFCallExpressionNode::getUDFName() const {
    auto constantValue = std::dynamic_pointer_cast<BasicValue>(udfName->getConstantValue());
    return constantValue->value;
}

}// namespace NES