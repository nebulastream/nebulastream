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

const std::string SinkLogicalOperatorNode::toString() const {
  std::stringstream ss;
  ss << "SINK(" << NES::toString(sink_) << ")";
  return ss.str();
}
/*
bool SinkLogicalOperatorNode::equal(const Node& rhs) const {
    try {
        auto& rhs_ = dynamic_cast<const SinkLogicalOperatorNode&>(rhs);
        return true;
    } catch (const std::bad_cast& e) {
        return false;
    }
}
*/
const NodePtr createSinkLogicalOperatorNode(const DataSinkPtr& sink) {
    return std::make_shared<SinkLogicalOperatorNode>(sink);
}
}
