#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>

namespace NES {
SinkLogicalOperatorNode::SinkLogicalOperatorNode(const DataSinkPtr sink) : sinkDescriptor(sink) {
}

DataSinkPtr SinkLogicalOperatorNode::getDataSink() {
    return sinkDescriptor;
}

bool SinkLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }
    if (rhs->instanceOf<SinkLogicalOperatorNode>()) {
        auto sinkOperator = rhs->as<SinkLogicalOperatorNode>();
        // todo check if the source is the same
        return true;
    }
    return false;
};

const std::string SinkLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SINK(" << NES::toString(sinkDescriptor) << ")";
    return ss.str();
}

LogicalOperatorNodePtr createSinkLogicalOperatorNode(const DataSinkPtr sink) {
    return std::make_shared<SinkLogicalOperatorNode>(sink);
}
}
