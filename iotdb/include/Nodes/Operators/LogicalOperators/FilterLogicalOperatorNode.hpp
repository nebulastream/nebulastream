#ifndef FILTER_LOGICAL_OPERATOR_NODE_HPP
#define FILTER_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <API/UserAPIExpression.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>

namespace NES {

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

/**
 * @brief Filter operator, which contains an expression as a predicate.
 */
class FilterLogicalOperatorNode : public LogicalOperatorNode {
  public:
    explicit FilterLogicalOperatorNode(const ExpressionNodePtr);
    ~FilterLogicalOperatorNode() = default;

    /**
     * @brief get the filter predicate.
     * @return PredicatePtr
     */
    ExpressionNodePtr getPredicate();

    /**
     * @brief check if two operators have the same filter predicate.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    bool inferSchema() override;
  private:
    ExpressionNodePtr predicate;

};
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

}      // namespace NES
#endif // FILTER_LOGICAL_OPERATOR_NODE_HPP
