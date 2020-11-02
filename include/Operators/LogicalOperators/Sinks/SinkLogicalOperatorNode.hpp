#ifndef SINK_LOGICAL_OPERATOR_NODE_HPP
#define SINK_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

/**
 * @brief Node representing logical sink operator
 */
class SinkLogicalOperatorNode : public LogicalOperatorNode {
  public:
    SinkLogicalOperatorNode(OperatorId id);
    SinkLogicalOperatorNode(const SinkDescriptorPtr sinkDescriptor, OperatorId id);
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
