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
    ss << "FILTER(" << outputSchema->toString() << ")";
    return ss.str();
}

FilterLogicalOperatorNodePtr FilterLogicalOperatorNode::copy() {

    const FilterLogicalOperatorNodePtr copiedOptr = std::make_shared<FilterLogicalOperatorNode>(this->getPredicate());
    copiedOptr->setId(this->getId());

    std::vector<NodePtr> parents = this->getParents();
    for (auto parent : parents) {
        copiedOptr->addParent(parent);
    }

    std::vector<NodePtr> children = this->getChildren();
    for (auto child: children) {
        copiedOptr->addChild(child);
    }
    return copiedOptr;
}

LogicalOperatorNodePtr createFilterLogicalOperatorNode(const ExpressionNodePtr predicate) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate);
}

bool FilterLogicalOperatorNode::inferSchema()  {
    OperatorNode::inferSchema();
    predicate->inferStamp(inputSchema);
    if(!predicate->isPredicate()){
        NES_THROW_RUNTIME_ERROR("FilterLogicalOperator: the filter expression is not a valid predicate");
        return false;
    }
    return true;
}

}
