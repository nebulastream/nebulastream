#ifndef NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
namespace NES{
/**
 * @brief This node represents a arithmetical expression.
 */
class ArithmeticalExpressionNode : public BinaryExpressionNode {
  protected:
    ArithmeticalExpressionNode(DataTypePtr stamp);
    ~ArithmeticalExpressionNode() = default;
};

}

#endif // NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
