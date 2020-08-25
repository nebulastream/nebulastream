#ifndef Merge_LOGICAL_OPERATOR_NODE_HPP
#define Merge_LOGICAL_OPERATOR_NODE_HPP

#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <memory>

namespace NES {

class MergeLogicalOperatorNode;
typedef std::shared_ptr<MergeLogicalOperatorNode> MergeLogicalOperatorNodePtr;

/**
 * @brief Merge operator, which contains an expression as a predicate.
 */
class MergeLogicalOperatorNode : public LogicalOperatorNode {
  public:
    explicit MergeLogicalOperatorNode();
    ~MergeLogicalOperatorNode() = default;

    bool isIdentical(NodePtr rhs) const override;

    const std::string toString() const override;

    //infer schema of two child operators
    bool inferSchema() override;

    OperatorNodePtr copy() override;
};
}// namespace NES
#endif// Merge_LOGICAL_OPERATOR_NODE_HPP