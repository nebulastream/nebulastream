#include <OperatorNodes/Impl/SinkLogicalOperatorNode.hpp>

namespace NES {
SinkLogicalOperatorNode::SinkLogicalOperatorNode(const DataSinkPtr sink) : sink_(sink) {
}

// SinkLogicalOperatorNode& SinkLogicalOperatorNode::operator=(const SinkLogicalOperatorNode& other) {
//     if (this != &other) {
//         sink_ = other.sink_;
//     }
//     return *this;
// }

DataSinkPtr SinkLogicalOperatorNode::getDataSinkPtr() {
    return sink_;
}

OperatorType SinkLogicalOperatorNode::getOperatorType() const {
    return OperatorType::SINK_OP;
}

const std::string SinkLogicalOperatorNode::toString() const {
  std::stringstream ss;
  ss << "SINK(" << NES::toString(sink_) << ")";
  return ss.str();
}

const BaseOperatorNodePtr createSinkLogicalOperatorNode(const DataSinkPtr& sink) {
    return std::make_shared<SinkLogicalOperatorNode>(sink);
}
}
