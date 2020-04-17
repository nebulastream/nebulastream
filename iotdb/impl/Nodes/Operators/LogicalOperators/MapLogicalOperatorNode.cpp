#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <API/Types/AttributeField.hpp>

namespace NES {

MapLogicalOperatorNode::MapLogicalOperatorNode(const FieldAssignmentExpressionNodePtr mapExpression) : mapExpression(mapExpression) {}

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
