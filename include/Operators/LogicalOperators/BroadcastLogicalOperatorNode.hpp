#ifndef SPLIT_LOGICAL_OPERATOR_NODE_HPP
#define SPLIT_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <memory>

namespace NES {

class BroadcastLogicalOperatorNode;
typedef std::shared_ptr<BroadcastLogicalOperatorNode> BroadcastLogicalOperatorNodePtr;

/**
 * @brief broadcast operator is used for efficiently broadcasting the tuples form its upstream operator to all its downstream operators.
 */
class BroadcastLogicalOperatorNode : public LogicalOperatorNode {
  public:
    explicit BroadcastLogicalOperatorNode(OperatorId id);
    ~BroadcastLogicalOperatorNode() = default;

    bool inferSchema() override;

    bool equal(const NodePtr rhs) const override;

    bool isIdentical(NodePtr rhs) const override;

    const std::string toString() const override;

    OperatorNodePtr copy() override;
};
}// namespace NES

#endif// SPLIT_LOGICAL_OPERATOR_NODE_HPP
