#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>

namespace NES {

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const ExpressionNodePtr predicate)
    : LogicalOperatorNode(), predicate(predicate) {
}

ExpressionNodePtr FilterLogicalOperatorNode::getPredicate() {
    return predicate;
}

bool FilterLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }
    if (rhs->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterOperator = rhs->as<FilterLogicalOperatorNode>();
        return predicate->equal(filterOperator->predicate);
    }
    return false;
};

const std::string FilterLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "FILTER(" << predicate->toString() << ")";
    return ss.str();
}

LogicalOperatorNodePtr createFilterLogicalOperatorNode(const ExpressionNodePtr predicate) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate);
}

}
