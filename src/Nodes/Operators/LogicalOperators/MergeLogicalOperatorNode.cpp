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
    ss << "Merge(" << outputSchema->toString() << ")";
    return ss.str();
}

LogicalOperatorNodePtr createMergeLogicalOperatorNode() {
    return std::make_shared<MergeLogicalOperatorNode>();
}

bool MergeLogicalOperatorNode::inferSchema() {
    OperatorNode::inferSchema();

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
    auto copy = createMergeLogicalOperatorNode();
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

}// namespace NES