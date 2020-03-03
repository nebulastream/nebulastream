#ifndef SINK_LOGICAL_OPERATOR_NODE_HPP
#define SINK_LOGICAL_OPERATOR_NODE_HPP
#include <memory>
#include <OperatorNodes/LogicalOperatorNode.hpp>
namespace NES {
class SinkLogicalOperatorNode : public LogicalOperatorNode,
                                public std::enable_shared_from_this<SinkLogicalOperatorNode> {
public:
    SinkLogicalOperatorNode();
    SinkLogicalOperatorNode(const DataSinkPtr sink);
    SinkLogicalOperatorNode& operator=(const SinkLogicalOperatorNode& other);
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    DataSinkPtr getDataSinkPtr();

    virtual BaseOperatorNodePtr makeShared() override { return shared_from_this(); };
    virtual bool equals(const BaseOperatorNode& rhs) const override;
private:
    DataSinkPtr sink_;
};
}
#endif  // SINK_LOGICAL_OPERATOR_NODE_HPP
