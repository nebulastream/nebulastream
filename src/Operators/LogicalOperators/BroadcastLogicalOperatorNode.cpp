#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>

namespace NES {

BroadcastLogicalOperatorNode::BroadcastLogicalOperatorNode() {}

bool BroadcastLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return rhs->as<BroadcastLogicalOperatorNode>()->getId() == id;
}

bool BroadcastLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<BroadcastLogicalOperatorNode>()) {
        return true;
    }
    return false;
};

bool BroadcastLogicalOperatorNode::inferSchema() {
    // infer the default input and output schema
    OperatorNode::inferSchema();
    return true;
}

const std::string BroadcastLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "BROADCAST(" << outputSchema->toString() << ")";
    return ss.str();
}

OperatorNodePtr BroadcastLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createBroadcastOperator();
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}
}// namespace NES
