#ifndef SINK_LOGICAL_OPERATOR_NODE_HPP
#define SINK_LOGICAL_OPERATOR_NODE_HPP

#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Node representing logical sink operator
 */
class SinkLogicalOperatorNode : public LogicalOperatorNode {
  public:
    SinkLogicalOperatorNode();
    SinkLogicalOperatorNode(const SinkDescriptorPtr sinkDescriptor);
    SinkLogicalOperatorNode& operator=(const SinkLogicalOperatorNode& other);
    bool isIdentical(NodePtr rhs) const override;
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    SinkDescriptorPtr getSinkDescriptor();
    void setSinkDescriptor(SinkDescriptorPtr sinkDescriptor);
    OperatorNodePtr copy() override;

  private:
    SinkDescriptorPtr sinkDescriptor;
};
}// namespace NES
#endif// SINK_LOGICAL_OPERATOR_NODE_HPP
