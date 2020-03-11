#ifndef SINK_LOGICAL_OPERATOR_NODE_HPP
#define SINK_LOGICAL_OPERATOR_NODE_HPP
#include <memory>
#include <OperatorNodes/LogicalOperatorNode.hpp>
namespace NES {
class SinkLogicalOperatorNode : public LogicalOperatorNode
                                 {
public:
    SinkLogicalOperatorNode();
    SinkLogicalOperatorNode(const DataSinkPtr sink);
    SinkLogicalOperatorNode& operator=(const SinkLogicalOperatorNode& other);
    const std::string toString() const override;
    DataSinkPtr getDataSinkPtr();

private:
    DataSinkPtr sink_;
};
}
#endif  // SINK_LOGICAL_OPERATOR_NODE_HPP
