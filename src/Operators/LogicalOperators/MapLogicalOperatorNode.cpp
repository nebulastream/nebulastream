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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>

namespace NES {

MapLogicalOperatorNode::MapLogicalOperatorNode(const FieldAssignmentExpressionNodePtr mapExpression, OperatorId id)
    : mapExpression(mapExpression), UnaryOperatorNode(id) {}

bool MapLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<MapLogicalOperatorNode>()->getId() == id;
}

bool MapLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<MapLogicalOperatorNode>()) {
        auto mapOperator = rhs->as<MapLogicalOperatorNode>();
        return mapExpression->equal(mapOperator->mapExpression);
    }
    return false;
};

bool MapLogicalOperatorNode::inferSchema() {
    // infer the default input and output schema
    UnaryOperatorNode::inferSchema();
    // use the default input schema to calculate the out schema of this operator.
    mapExpression->inferStamp(getInputSchema());

    auto assignedField = mapExpression->getField();
    std::string fieldName = assignedField->getFieldName();
    if (fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos) {
        fieldName = inputSchema->getQualifierNames() + fieldName;
    }

    if (outputSchema->hasFullyQualifiedFieldName(fieldName)) {
        // The assigned field is part of the current schema.
        // Thus we check if it has the correct type.
        NES_TRACE("MAP Logical Operator: the field " << fieldName << " is already in the schema, so we updated its type.");
        outputSchema->replaceField(fieldName, assignedField->getStamp());
    } else {
        // The assigned field is not part of the current schema.
        // Thus we extend the schema by the new attribute.
        NES_TRACE("MAP Logical Operator: the field " << fieldName << " is not part of the schema, so we added it.");
        outputSchema->addField(fieldName, assignedField->getStamp());
    }
    return true;
}

const std::string MapLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP(" << id << ")";
    return ss.str();
}

std::string MapLogicalOperatorNode::getStringBasedSignature() {
    return "MAP(" + mapExpression->toString() + ")." + children[0]->as<LogicalOperatorNode>()->getStringBasedSignature();
}

FieldAssignmentExpressionNodePtr MapLogicalOperatorNode::getMapExpression() { return mapExpression; }

OperatorNodePtr MapLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createMapOperator(mapExpression, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setSignature(signature);
    return copy;
}
}// namespace NES
