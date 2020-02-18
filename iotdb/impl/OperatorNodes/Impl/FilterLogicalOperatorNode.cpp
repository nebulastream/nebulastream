#include <OperatorNodes/Impl/FilterLogicalOperatorNode.hpp>

namespace NES {

FilterLogicalOperatorNode::FilterLogicalOperatorNode() {
    std::cout << "Call default FilterLogicalOperatorNode constructor" << std::endl;
}

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const PredicatePtr predicate) : predicate_(predicate) {

}

FilterLogicalOperatorNode::FilterLogicalOperatorNode(FilterLogicalOperatorNode& other) {
    if (this != &other) {
        // do some update
        std::cout << "not equal" << std::endl;
    }
    // return *this;
}

// FilterLogicalOperatorNode& FilterLogicalOperatorNode::operator=(FilterLogicalOperatorNode& other) {
//     if (this != &other) {
//         std::cout << "not equal" << std::endl;
//     }
//     return *this;
// }

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const OperatorPtr op) {
    std::cout << "op: " << op->toString() << std::endl;
}

// const BaseOperatorNodePtr LogicalOperatorNode::copy() {
//     return std::make_shared<FilterLogicalOperatorNode>();
// }

OperatorType FilterLogicalOperatorNode::getOperatorType() const {
    return OperatorType::FILTER_OP;
}
const std::string FilterLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "FILTER(" << NES::toString(predicate_) << ")";
    return ss.str();
}

const BaseOperatorNodePtr createFilterLogicalOperatorNode(const PredicatePtr& predicate) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate);
}


}
