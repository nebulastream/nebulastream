#ifndef SINK_LOGICAL_OPERATOR_NODE_HPP
#define SINK_LOGICAL_OPERATOR_NODE_HPP

#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {
class SinkLogicalOperatorNode : public LogicalOperatorNode {
  public:
    SinkLogicalOperatorNode();
    SinkLogicalOperatorNode(const SinkDescriptorPtr sinkDescriptor);
    SinkLogicalOperatorNode& operator=(const SinkLogicalOperatorNode& other);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    SinkDescriptorPtr getSinkDescriptor();

  private:
    SinkDescriptorPtr sinkDescriptor;
};
}
#endif  // SINK_LOGICAL_OPERATOR_NODE_HPP
