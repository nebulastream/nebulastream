#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Optimizer/Utils/OperatorToZ3ExprUtil.hpp>
#include <z3++.h>

namespace NES {

MapLogicalOperatorNode::MapLogicalOperatorNode(const FieldAssignmentExpressionNodePtr mapExpression, OperatorId id)
    : mapExpression(mapExpression), LogicalOperatorNode(id) {}

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
    ss << "MAP(" << id << ")";
    return ss.str();
}

FieldAssignmentExpressionNodePtr MapLogicalOperatorNode::getMapExpression() {
    return mapExpression;
}

OperatorNodePtr MapLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createMapOperator(mapExpression, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}
z3::expr MapLogicalOperatorNode::getZ3Expression(z3::context& context) {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    return OperatorToZ3ExprUtil::createForOperator(operatorNode, context);
}
}// namespace NES
