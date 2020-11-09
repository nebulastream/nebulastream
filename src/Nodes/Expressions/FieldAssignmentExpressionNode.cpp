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
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <memory>
#include <utility>
namespace NES {
FieldAssignmentExpressionNode::FieldAssignmentExpressionNode(DataTypePtr stamp)
    : BinaryExpressionNode(std::move(stamp)){};

FieldAssignmentExpressionNode::FieldAssignmentExpressionNode(FieldAssignmentExpressionNode* other) : BinaryExpressionNode(other){};

FieldAssignmentExpressionNodePtr FieldAssignmentExpressionNode::create(FieldAccessExpressionNodePtr fieldAccess,
                                                                       ExpressionNodePtr expressionNodePtr) {
    auto fieldAssignment = std::make_shared<FieldAssignmentExpressionNode>(expressionNodePtr->getStamp());
    fieldAssignment->setChildren(fieldAccess, expressionNodePtr);
    return fieldAssignment;
}

bool FieldAssignmentExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<FieldAssignmentExpressionNode>()) {
        auto otherFieldAssignment = rhs->as<FieldAssignmentExpressionNode>();
        // a field assignment expression has always two children.
        return getField()->equal(otherFieldAssignment->getField())
            && getAssignment()->equal(otherFieldAssignment->getAssignment());
    }
    return false;
}

const std::string FieldAssignmentExpressionNode::toString() const {
    return "FieldAssignment()";
}

FieldAccessExpressionNodePtr FieldAssignmentExpressionNode::getField() const {
    return getLeft()->as<FieldAccessExpressionNode>();
}

ExpressionNodePtr FieldAssignmentExpressionNode::getAssignment() const {
    return getRight();
}
void FieldAssignmentExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp of assignment expression
    getAssignment()->inferStamp(schema);

    // field access
    auto field = getField();
    if (field->getStamp()->isUndefined()) {
        // if the field has no stamp set it to the one of the assignment
        field->setStamp(getAssignment()->getStamp());
    } else {
        // the field already has a type, check if it is compatible with the assignment
        field->getStamp()->isEquals(getAssignment()->getStamp());
    }
}
ExpressionNodePtr FieldAssignmentExpressionNode::copy() {
    return std::make_shared<FieldAssignmentExpressionNode>(FieldAssignmentExpressionNode(this));
}

}// namespace NES