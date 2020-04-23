#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <API/Types/AttributeField.hpp>

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

SchemaPtr MapLogicalOperatorNode::getResultSchema() const {
    // infer result schema
    auto resultSchema = OperatorNode::getResultSchema()->copy();
    mapExpression->inferStamp(resultSchema);
    auto assignedField = mapExpression->getField();
    if (resultSchema->has(assignedField->getFieldName())) {
        // The assigned field is part of the current schema.
        // Thus we check if it has the correct type.
        if (assignedField->getStamp()->isEqual(
            resultSchema->get(assignedField->getFieldName())->getDataType())) {
            NES_DEBUG(
                "MAP Logical Operator: the field " << assignedField->getFieldName() << " is already of the schema");
        } else {
            NES_THROW_RUNTIME_ERROR("MAP Logical Operator: the field " + assignedField->getFieldName()
                                                               + " is part of the schema, but the data type is not equal");
        }
    } else {
        // The assigned field is not part of the default schema.
        // Thus we extend the schema by the new attribute.
        resultSchema->addField(assignedField->getFieldName(), assignedField->getStamp());
    }
    return resultSchema;
}

const std::string MapLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP()";
    return ss.str();
}

FieldAssignmentExpressionNodePtr MapLogicalOperatorNode::getMapExpression() {
    return mapExpression;
}

LogicalOperatorNodePtr createMapLogicalOperatorNode(const FieldAssignmentExpressionNodePtr mapExpression) {
    return std::make_shared<MapLogicalOperatorNode>(mapExpression);
}
}
