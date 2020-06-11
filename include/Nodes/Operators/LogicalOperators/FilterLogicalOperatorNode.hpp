#ifndef FILTER_LOGICAL_OPERATOR_NODE_HPP
#define FILTER_LOGICAL_OPERATOR_NODE_HPP

#include <API/UserAPIExpression.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <memory>

namespace NES {

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class FilterLogicalOperatorNode;
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

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

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @throws Exception the predicate expression has to return a boolean.
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;

    /**
     * @brief Create copy of this filter operator
     * @return copy of the filter operator
     */
    FilterLogicalOperatorNodePtr deepCopy();

    OperatorNodePtr copy() override;

  private:
    ExpressionNodePtr predicate;
};

}// namespace NES
#endif// FILTER_LOGICAL_OPERATOR_NODE_HPP
