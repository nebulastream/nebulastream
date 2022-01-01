/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Common/ValueTypes/ValueType.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>

namespace NES {
ConstantValueExpressionNode::ConstantValueExpressionNode(ValueTypePtr const& constantValue)
    : ExpressionNode(constantValue->dataType), constantValue(constantValue){};

ConstantValueExpressionNode::ConstantValueExpressionNode(ConstantValueExpressionNode* other)
    : ExpressionNode(other->constantValue->dataType), constantValue(other->constantValue) {}

bool ConstantValueExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<ConstantValueExpressionNode>()) {
        auto otherConstantValueNode = rhs->as<ConstantValueExpressionNode>();
        return otherConstantValueNode->constantValue->isEquals(constantValue);
    }
    return false;
}

std::string ConstantValueExpressionNode::toString() const { return "ConstantValue(" + constantValue->toString() + ")"; }

ExpressionNodePtr ConstantValueExpressionNode::create(ValueTypePtr const& constantValue) {
    return std::make_shared<ConstantValueExpressionNode>(ConstantValueExpressionNode(constantValue));
}

ValueTypePtr ConstantValueExpressionNode::getConstantValue() const { return constantValue; }

void ConstantValueExpressionNode::inferStamp(SchemaPtr) {
    // the stamp of constant value expressions is defined by the constant value type.
    // thus ut is already assigned correctly when the expression node is created.
}
ExpressionNodePtr ConstantValueExpressionNode::copy() {
    return std::make_shared<ConstantValueExpressionNode>(ConstantValueExpressionNode(this));
}

}// namespace NES