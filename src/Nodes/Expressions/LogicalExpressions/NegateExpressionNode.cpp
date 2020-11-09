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


#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
namespace NES {

NegateExpressionNode::NegateExpressionNode() : LogicalUnaryExpressionNode() {}

NegateExpressionNode::NegateExpressionNode(NegateExpressionNode* other) : LogicalUnaryExpressionNode(other) {}

bool NegateExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<NegateExpressionNode>()) {
        auto other = rhs->as<NegateExpressionNode>();
        return this->getChildren()[0]->equal(other->getChildren()[0]);
    }
    return false;
}

const std::string NegateExpressionNode::toString() const {
    return "NegateNode(" + stamp->toString() + ")";
}

ExpressionNodePtr NegateExpressionNode::create(const ExpressionNodePtr child) {
    auto equals = std::make_shared<NegateExpressionNode>();
    equals->setChild(child);
    return equals;
}

void NegateExpressionNode::inferStamp(SchemaPtr schema) {
    // delegate stamp inference of children
    ExpressionNode::inferStamp(schema);
    // check if children stamp is correct
    if (!child()->isPredicate()) {
        NES_THROW_RUNTIME_ERROR(
            "Negate Expression Node: the stamp of child must be boolean, but was: " + child()->getStamp()->toString());
    }
}
ExpressionNodePtr NegateExpressionNode::copy() {
    return std::make_shared<NegateExpressionNode>(NegateExpressionNode(this));
}

}// namespace NES