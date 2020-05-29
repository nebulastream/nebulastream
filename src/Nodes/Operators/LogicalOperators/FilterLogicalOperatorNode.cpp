#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Util/Logger.hpp>

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

FilterLogicalOperatorNodePtr FilterLogicalOperatorNode::makeACopy() {

    NES_INFO("FilterLogicalOperatorNode: Create copy of the filter operator");
    const FilterLogicalOperatorNodePtr copiedOptr = std::make_shared<FilterLogicalOperatorNode>(this->getPredicate());
    copiedOptr->setId(this->getId());

    NES_INFO("FilterLogicalOperatorNode: copy all parents");
    std::vector<NodePtr> parents = this->getParents();
    for (auto parent : parents) {
        if (!copiedOptr->addParent(parent)) {
            NES_THROW_RUNTIME_ERROR("FilterLogicalOperatorNode: Unable to add parent to copy");
        }
    }

    NES_INFO("FilterLogicalOperatorNode: copy all children");
    std::vector<NodePtr> children = this->getChildren();
    for (auto child : children) {
        if (!copiedOptr->addChild(child)) {
            NES_THROW_RUNTIME_ERROR("FilterLogicalOperatorNode: Unable to add child to copy");
        }
    }
    return copiedOptr;
}

LogicalOperatorNodePtr createFilterLogicalOperatorNode(const ExpressionNodePtr predicate) {
    return std::make_shared<FilterLogicalOperatorNode>(predicate);
}

bool FilterLogicalOperatorNode::inferSchema() {
    OperatorNode::inferSchema();
    predicate->inferStamp(inputSchema);
    if (!predicate->isPredicate()) {
        NES_THROW_RUNTIME_ERROR("FilterLogicalOperator: the filter expression is not a valid predicate");
        return false;
    }
    return true;
}

}// namespace NES
