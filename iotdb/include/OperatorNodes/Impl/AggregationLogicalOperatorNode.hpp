#ifndef AGG_LOGICAL_OPERATOR_NODE_HPP
#define AGG_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <OperatorNodes/LogicalOperatorNode.hpp>

namespace NES {
class AggregationLogicalOperatorNode : public LogicalOperatorNode,
                                       public std::enable_shared_from_this<AggregationLogicalOperatorNode> {
public:
    AggregationLogicalOperatorNode(const AggregationSpec& aggrSpec);
    ~AggregationLogicalOperatorNode();
    const std::string toString() const override;
    OperatorType getOperatorType() const override;

    virtual NodePtr makeShared() override { return shared_from_this(); };
    virtual bool equals(const Node& rhs) const override;

private:
    AggregationSpec aggrSpec_;
};
}
#endif  // AGG_LOGICAL_OPERATOR_NODE_HPP
