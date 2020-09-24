#ifndef SPLIT_LOGICAL_OPERATOR_NODE_HPP
#define SPLIT_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <memory>

namespace NES {

class BroadcastLogicalOperatorNode;
typedef std::shared_ptr<BroadcastLogicalOperatorNode> BroadcastLogicalOperatorNodePtr;

/**
 * @brief Split operator, which contains an field assignment expression that manipulates a field of the record.
 */
class BroadcastLogicalOperatorNode : public LogicalOperatorNode {
  public:

    static BroadcastLogicalOperatorNodePtr create();

    /**
     * @brief Infers the schema of the map operator. We support two cases:
     * 1. the assignment statement manipulates a already existing field. In this case the data type of the field can change.
     * 2. the assignment statement creates a new field with an inferred data type.
     * @throws throws exception if inference was not possible.
     * @return true if inference was possible
     */
    bool inferSchema() override;

    bool equal(const NodePtr rhs) const override;

    bool isIdentical(NodePtr rhs) const override;

    const std::string toString() const override;

    OperatorNodePtr copy() override;

  private:
    explicit BroadcastLogicalOperatorNode();

};
}// namespace NES

#endif// SPLIT_LOGICAL_OPERATOR_NODE_HPP
