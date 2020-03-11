#include <OperatorNodes/Impl/FilterLogicalOperatorNode.hpp>

namespace NES {

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const PredicatePtr& predicate)
    : LogicalOperatorNode(), predicate_(NES::copy(predicate)) {

}

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const FilterLogicalOperatorNode* other)
    : LogicalOperatorNode(), predicate_(NES::copy(other->predicate_)) {
}

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const FilterLogicalOperatorNode& other)
    : LogicalOperatorNode(), predicate_(NES::copy(other.predicate_)) {
}

bool FilterLogicalOperatorNode::equal(const NodePtr& rhs) const {
    if (rhs->instanceOf<FilterLogicalOperatorNode>()) {
        auto rhs_ = rhs->as<FilterLogicalOperatorNode>();
        return predicate_->equals(*rhs_->predicate_.get());
    }
    return false;
};

const std::string FilterLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "FILTER(" << predicate_->toString() << ")";
    return ss.str();
}

NodePtr createFilterLogicalOperatorNode(const PredicatePtr& predicate) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate);
}
}
