#include <OperatorNodes/Impl/FilterLogicalOperatorNode.hpp>

namespace NES {

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const PredicatePtr& predicate)
    : LogicalOperatorNode(), predicate_(NES::copy(predicate)) {

}

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const FilterLogicalOperatorNode& other) {
    std::cout << "other: " << &other
              << ", this: " << this
              << std::endl;
    if (this != &other) {
        // not the same object, do some update
        std::cout << "not equal" << std::endl;
    }
}

bool FilterLogicalOperatorNode::equals(const BaseOperatorNode& rhs) const {
    try {
        auto& rhs_ = dynamic_cast<const FilterLogicalOperatorNode &>(rhs);
        return predicate_->equals(*rhs_.predicate_.get());
    } catch (const std::bad_cast& e) {
        return false;
    }
}

BaseOperatorNodePtr FilterLogicalOperatorNode::makeShared() {
    // std::cout << "call filter-logical-operator-node's make shared" << std::endl;
    return shared_from_this();
}

OperatorType FilterLogicalOperatorNode::getOperatorType() const {
    return OperatorType::FILTER_OP;
}
const std::string FilterLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "FILTER(" << predicate_->toString() << ")";
    return ss.str();
}

const BaseOperatorNodePtr createFilterLogicalOperatorNode(const PredicatePtr& predicate) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate);
}

template
FilterLogicalOperatorNode& BaseOperatorNode::as<FilterLogicalOperatorNode>();

}
