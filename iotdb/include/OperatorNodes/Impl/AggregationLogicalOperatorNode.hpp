#ifndef AGG_LOGICAL_OPERATOR_NODE_HPP
#define AGG_LOGICAL_OPERATOR_NODE_HPP

#include <OperatorNodes/LogicalOperatorNode.hpp>

namespace NES {
class AggregationLogicalOperatorNode : public LogicalOperatorNode {
public:
    AggregationLogicalOperatorNode(const AggregationSpec& aggrSpec);
    ~AggregationLogicalOperatorNode();
    // AggregationLogicalOperatorNode& operator=(const AggregationLogicalOperatorNode& other);
    const std::string toString() const override;
    OperatorType getOperatorType() const override;

private:
    AggregationSpec aggrSpec_;
};
}
#endif  // AGG_LOGICAL_OPERATOR_NODE_HPP
