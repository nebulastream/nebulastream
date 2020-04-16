#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Types/AttributeField.hpp>


namespace NES {
MapLogicalOperatorNode::MapLogicalOperatorNode(const ExpressionNodePtr mapExpression) : mapExpression(mapExpression) {
}

bool MapLogicalOperatorNode::equal(const NodePtr rhs) const {
    if(this->isIdentical(rhs)) {
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

LogicalOperatorNodePtr createMapLogicalOperatorNode(const ExpressionNodePtr mapExpression) {
    return std::make_shared<MapLogicalOperatorNode>(mapExpression);
}
}
