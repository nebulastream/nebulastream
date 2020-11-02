#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/Utils/OperatorToZ3ExprUtil.hpp>
#include <utility>
#include <z3++.h>

namespace NES {
SinkLogicalOperatorNode::SinkLogicalOperatorNode(const SinkDescriptorPtr sinkDescriptor, OperatorId id)
    : sinkDescriptor(sinkDescriptor), LogicalOperatorNode(id) {}

SinkDescriptorPtr SinkLogicalOperatorNode::getSinkDescriptor() {
    return sinkDescriptor;
}

void SinkLogicalOperatorNode::setSinkDescriptor(SinkDescriptorPtr sinkDescriptor) {
    this->sinkDescriptor = std::move(sinkDescriptor);
}

bool SinkLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<SinkLogicalOperatorNode>()->getId() == id;
}

bool SinkLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<SinkLogicalOperatorNode>()) {
        auto sinkOperator = rhs->as<SinkLogicalOperatorNode>();
        return sinkOperator->getSinkDescriptor()->equal(sinkDescriptor);
    }
    return false;
};

const std::string SinkLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SINK(" << id << ")";
    return ss.str();
}

OperatorNodePtr SinkLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createSinkOperator(sinkDescriptor, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

z3::expr SinkLogicalOperatorNode::getZ3Expression(z3::context& context) {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    return OperatorToZ3ExprUtil::createForOperator(operatorNode, context);
}
}// namespace NES
