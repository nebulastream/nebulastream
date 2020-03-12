#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>

namespace NES {

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const PredicatePtr& predicate)
    : LogicalOperatorNode(), predicate(NES::copy(predicate)) {
}

PredicatePtr FilterLogicalOperatorNode::getPredicate() {
    return predicate;
}

bool FilterLogicalOperatorNode::equal(const NodePtr& rhs) const {
    if (rhs->instanceOf<FilterLogicalOperatorNode>()) {
        auto rhs_ = rhs->as<FilterLogicalOperatorNode>();
        return predicate->equals(*rhs_->predicate.get());
    }
    return false;
};

const std::string FilterLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "FILTER(" << predicate->toString() << ")";
    return ss.str();
}

NodePtr createFilterLogicalOperatorNode(const PredicatePtr& predicate) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate);
}
}
