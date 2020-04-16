#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_GREATERTHENEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_GREATERTHENEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node represents a greater comparision between the two children.
 */
class GreaterThenExpressionNode : public LogicalBinaryExpressionNode {
  public:
    GreaterThenExpressionNode();
    ~GreaterThenExpressionNode() = default;
    /**
    * @brief Create a new greater then expression
    */
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_GREATERTHENEXPRESSIONNODE_HPP_
