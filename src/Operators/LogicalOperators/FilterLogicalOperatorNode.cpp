#include <API/Schema.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Util/Logger.hpp>
#include <deque>

namespace NES {

FilterLogicalOperatorNode::FilterLogicalOperatorNode(const ExpressionNodePtr predicate)
    : LogicalOperatorNode(), predicate(predicate) {
}

ExpressionNodePtr FilterLogicalOperatorNode::getPredicate() {
    return predicate;
}

bool FilterLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<FilterLogicalOperatorNode>()->getId() == id;
}

bool FilterLogicalOperatorNode::equal(const NodePtr rhs) const {

    if (rhs->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterOperator = rhs->as<FilterLogicalOperatorNode>();
        return predicate->equal(filterOperator->predicate);
    }
    return false;
};

const std::string FilterLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "FILTER(" << id << ")";
    return ss.str();
}

bool FilterLogicalOperatorNode::inferSchema() {
    OperatorNode::inferSchema();
    predicate->inferStamp(inputSchema);
    if (!predicate->isPredicate()) {
        NES_THROW_RUNTIME_ERROR("FilterLogicalOperator: the filter expression is not a valid predicate");
    }
    return true;
}

OperatorNodePtr FilterLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createFilterOperator(predicate);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

}// namespace NES
