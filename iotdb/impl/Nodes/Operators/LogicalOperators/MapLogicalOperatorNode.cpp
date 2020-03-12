#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <API/Types/AttributeField.hpp>


namespace NES {
MapLogicalOperatorNode::MapLogicalOperatorNode(const AttributeFieldPtr field, const PredicatePtr predicate) : field_(
    field), predicate_(predicate) {
}

const std::string MapLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP_UDF(" << field_->toString() << " = " << NES::toString(predicate_) << ")";
    return ss.str();
}

NodePtr createMapLogicalOperatorNode(const AttributeFieldPtr& field, const PredicatePtr& predicate) {
    return std::make_shared<MapLogicalOperatorNode>(field, predicate);
}
}
