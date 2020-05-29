#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <utility>

namespace NES {
SinkLogicalOperatorNode::SinkLogicalOperatorNode(const SinkDescriptorPtr sinkDescriptor) : sinkDescriptor(sinkDescriptor) {
}

SinkDescriptorPtr SinkLogicalOperatorNode::getSinkDescriptor() {
    return sinkDescriptor;
}

void SinkLogicalOperatorNode::setSinkDescriptor(SinkDescriptorPtr sinkDescriptor) {
    this->sinkDescriptor = std::move(sinkDescriptor);
}

bool SinkLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }
    if (rhs->instanceOf<SinkLogicalOperatorNode>()) {
        auto sinkOperator = rhs->as<SinkLogicalOperatorNode>();
        return true;
    }
    return false;
};

const std::string SinkLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SINK(" << outputSchema->toString() << ")";
    return ss.str();
}

LogicalOperatorNodePtr createSinkLogicalOperatorNode(const SinkDescriptorPtr sinkDescriptor) {
    return std::make_shared<SinkLogicalOperatorNode>(sinkDescriptor);
}
}// namespace NES
