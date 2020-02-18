#include <OperatorNodes/Impl/MapLogicalOperatorNode.hpp>

namespace NES {
MapLogicalOperatorNode::MapLogicalOperatorNode(const AttributeFieldPtr field, const PredicatePtr predicate) : field_(field), predicate_(predicate) {
}


OperatorType MapLogicalOperatorNode::getOperatorType() const {
    return OperatorType::MAP_OP;
}

const std::string MapLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "MAP_UDF(" << field_->toString() << " = " << NES::toString(predicate_) << ")";
    return ss.str();
}

const BaseOperatorNodePtr createMapLogicalOperatorNode(const AttributeFieldPtr& field, const PredicatePtr& predicate) {
    return std::make_shared<MapLogicalOperatorNode>(field, predicate);
}
}
