#include <API/Schema.hpp>
#include <API/Types/AttributeField.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>

namespace NES {

MapLogicalOperatorNode::MapLogicalOperatorNode(const FieldAssignmentExpressionNodePtr mapExpression) : mapExpression(
    mapExpression) {}

bool MapLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }
    if (rhs->instanceOf<MapLogicalOperatorNode>()) {
        auto mapOperator = rhs->as<MapLogicalOperatorNode>();
        return mapExpression->equal(mapOperator->mapExpression);
    }
    return false;
};

bool MapLogicalOperatorNode::inferSchema() {
    // infer the default input and output schema
    OperatorNode::inferSchema();
    // use the default input schema to calculate the out schema of this operator.
    mapExpression->inferStamp(getInputSchema());
    auto assignedField = mapExpression->getField();
    if (outputSchema->has(assignedField->getFieldName())) {
        // The assigned field is part of the current schema.
        // Thus we check if it has the correct type.
        outputSchema->replaceField(assignedField->getFieldName(), assignedField->getStamp());
        NES_DEBUG(
            "MAP Logical Operator: the field " << assignedField->getFieldName() << " is already of the schema, so we updated its type.");
    } else {
        // The assigned field is not part of the current schema.
        // Thus we extend the schema by the new attribute.
        outputSchema->addField(assignedField->getFieldName(), assignedField->getStamp());
        NES_DEBUG(
            "MAP Logical Operator: the field " << assignedField->getFieldName() << " is not part of the schema, so we added it.");
    }
    return true;
}

const std::string MapLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP(" << outputSchema->toString() << ")";
    return ss.str();
}

FieldAssignmentExpressionNodePtr MapLogicalOperatorNode::getMapExpression() {
    return mapExpression;
}

LogicalOperatorNodePtr createMapLogicalOperatorNode(const FieldAssignmentExpressionNodePtr mapExpression) {
    return std::make_shared<MapLogicalOperatorNode>(mapExpression);
}
}// namespace NES
