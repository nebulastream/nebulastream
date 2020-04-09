#ifndef SINK_LOGICAL_OPERATOR_NODE_HPP
#define SINK_LOGICAL_OPERATOR_NODE_HPP
#include <memory>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
namespace NES {
class SinkLogicalOperatorNode : public LogicalOperatorNode {
  public:
    SinkLogicalOperatorNode();
    SinkLogicalOperatorNode(const DataSinkPtr sink);
    SinkLogicalOperatorNode& operator=(const SinkLogicalOperatorNode& other);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    DataSinkPtr getDataSink();

  private:
    DataSinkPtr sink;
};
}
#endif  // SINK_LOGICAL_OPERATOR_NODE_HPP
