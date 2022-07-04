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
#include <Nodes/Expressions/UdfCallExpressions/UdfCallExpressionNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <utility>

namespace NES {

UdfCallExpressionNode::UdfCallExpressionNode() : ExpressionNode(DataTypeFactory::createBoolean()) {}

UdfCallExpressionNode::UdfCallExpressionNode(UdfCallExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getUdfName()->copy());
}

ExpressionNodePtr UdfCallExpressionNode::create(const ConstantValueExpressionNodePtr& udfName,
                                                std::vector<ExpressionNodePtr> functionArgs) {
    auto udfExpressionNode = std::make_shared<UdfCallExpressionNode>();
    udfExpressionNode->setChildren(udfName, std::move(functionArgs));
    return udfExpressionNode;
}

void UdfCallExpressionNode::setChildren(const ExpressionNodePtr& udfName, std::vector<ExpressionNodePtr> functionArgs) {
    addChild(udfName);
    functionArguments = std::move(functionArgs);
}

void UdfCallExpressionNode::inferStamp(const Optimizer::TypeInferencePhaseContext& ctx, SchemaPtr schema) {
    auto left = getUdfName();
    left->inferStamp(ctx, schema);
    ctx.getUdfCatalog()->
    if (!left->getStamp()->isCharArray()) {
        throw std::logic_error(
            "UdfCallExpressionNode: Error during stamp inference. Type needs to be Text but Left was:"
            + left->getStamp()->toString());
    }
    if (pythonUdfDescriptorPtr == nullptr) {
        throw std::logic_error("UdfCallExpressionNode: Error during stamp inference. No UdfDescriptor was set");
    }
    else {
        stamp = pythonUdfDescriptorPtr->getReturnType();
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

ExpressionNodePtr UdfCallExpressionNode::getUdfName() {
    return children[0]->as<ConstantValueExpressionNode>();
}

std::vector<ExpressionNodePtr> UdfCallExpressionNode::getFunctionArguments() {
    return functionArguments;
}

void UdfCallExpressionNode::setPythonUdfDescriptorPtr(const Catalogs::PythonUdfDescriptorPtr& pyUdfDescriptor) {
    pythonUdfDescriptorPtr = pyUdfDescriptor;
}

}// namespace NES::Experimental
#endif