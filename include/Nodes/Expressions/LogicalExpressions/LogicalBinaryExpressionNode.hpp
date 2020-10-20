#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LogicalExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents a logical unary expression.
 */
class LogicalBinaryExpressionNode : public BinaryExpressionNode, public LogicalExpressionNode {
  public:
    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override = 0;

    bool equal(const NodePtr rhs) const override;

  protected:
    LogicalBinaryExpressionNode();
    ~LogicalBinaryExpressionNode() = default;
    explicit LogicalBinaryExpressionNode(LogicalBinaryExpressionNode* other);
};
}// namespace NES

#endif//NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
