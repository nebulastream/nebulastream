#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Optimizer/Utils/OperatorToZ3ExprUtil.hpp>
#include <z3++.h>

namespace NES {

BroadcastLogicalOperatorNode::BroadcastLogicalOperatorNode(OperatorId id) : LogicalOperatorNode(id) {}

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
    auto copy = LogicalOperatorFactory::createBroadcastOperator(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

z3::expr BroadcastLogicalOperatorNode::getZ3Expression() {
    // create a context
    z3::context c;
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    return OperatorToZ3ExprUtil::createForOperator(operatorNode, c);
}
}// namespace NES
