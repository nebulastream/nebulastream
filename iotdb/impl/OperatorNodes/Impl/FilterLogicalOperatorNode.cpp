#include <OperatorNodes/Impl/FilterLogicalOperatorNode.hpp>

namespace NES {

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const PredicatePtr& predicate)
    : LogicalOperatorNode(), predicate_(NES::copy(predicate)) {

}

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const FilterLogicalOperatorNode& other) {
    if (this != &other) {
        // do some update
        std::cout << "not equal" << std::endl;
    }
    // return *this;
}

bool FilterLogicalOperatorNode::equals(const BaseOperatorNode& rhs) const {
    try {
        auto& rhs_ = dynamic_cast<const FilterLogicalOperatorNode &>(rhs);
        return predicate_->equals(*rhs_.predicate_.get());
    } catch (const std::bad_cast& e) {
        return false;
    }
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


}
