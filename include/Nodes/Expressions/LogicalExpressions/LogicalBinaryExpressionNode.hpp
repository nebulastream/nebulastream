#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LogicalExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents a logical unary expression.
 */
class LogicalBinaryExpressionNode : public BinaryExpressionNode, public LogicalExpressionNode {
  protected:
    LogicalBinaryExpressionNode();
    ~LogicalBinaryExpressionNode() = default;

  public:
    bool equal(const NodePtr rhs) const override;
};
}// namespace NES

#endif//NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
