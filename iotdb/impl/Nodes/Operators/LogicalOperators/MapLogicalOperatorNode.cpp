#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Types/AttributeField.hpp>


namespace NES {
MapLogicalOperatorNode::MapLogicalOperatorNode(const AttributeFieldPtr field, const PredicatePtr predicate) : field(
    field), predicate(predicate) {
}

bool MapLogicalOperatorNode::equal(const NodePtr rhs) const {
    if(this->isIdentical(rhs)) {
        return true;
    }
    if (rhs->instanceOf<MapLogicalOperatorNode>()) {
        auto mapOperator = rhs->as<MapLogicalOperatorNode>();
        return this->predicate->equals(*mapOperator->predicate.get()) && this->field->isEqual(mapOperator->field);
    }
    return false;
};

const std::string MapLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP(" << field->toString() << " = " << NES::toString(predicate) << ")";
    return ss.str();
}

LogicalOperatorNodePtr createMapLogicalOperatorNode(const AttributeFieldPtr field, const PredicatePtr predicate) {
    return std::make_shared<MapLogicalOperatorNode>(field, predicate);
}
}
