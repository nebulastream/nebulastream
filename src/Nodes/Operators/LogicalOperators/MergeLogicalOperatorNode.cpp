#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Util/Logger.hpp>

namespace NES {

MergeLogicalOperatorNode::MergeLogicalOperatorNode()
    : LogicalOperatorNode() {
}

bool MergeLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<MergeLogicalOperatorNode>()->getId() == id;
}

const std::string MergeLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "Merge(" << id << ")";
    return ss.str();
}

bool MergeLogicalOperatorNode::inferSchema() {
    OperatorNode::inferSchema();
    if (getChildren().size() != 2) {
        NES_THROW_RUNTIME_ERROR("MergeLogicalOperator: merge need two child operators.");
        return false;
    }
    auto child1 = getChildren()[0]->as<LogicalOperatorNode>();
    auto child2 = getChildren()[1]->as<LogicalOperatorNode>();

    auto schema1 = child1->getInputSchema();
    auto schema2 = child2->getInputSchema();

    if (!schema1->equals(schema2)) {
        NES_THROW_RUNTIME_ERROR("MergeLogicalOperator: the two input streams have different schema.");
        return false;
    }
    return true;
}

OperatorNodePtr MergeLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createMergeOperator();
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

bool MergeLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<MergeLogicalOperatorNode>()) {
        return true;
    }
    return false;
}

}// namespace NES