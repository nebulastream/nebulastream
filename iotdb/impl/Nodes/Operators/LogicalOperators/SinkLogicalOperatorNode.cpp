#include <Nodes/Operators/LogicalOperators/SinkLogicalOperatorNode.hpp>

namespace NES {
SinkLogicalOperatorNode::SinkLogicalOperatorNode(const DataSinkPtr sink) : sink(sink) {
}

DataSinkPtr SinkLogicalOperatorNode::getDataSink() {
    return sink;
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
    ss << "SINK(" << NES::toString(sink) << ")";
    return ss.str();
}

NodePtr createSinkLogicalOperatorNode(const DataSinkPtr& sink) {
    return std::make_shared<SinkLogicalOperatorNode>(sink);
}
}
