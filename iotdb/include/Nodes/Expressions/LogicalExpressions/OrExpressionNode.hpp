#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_OREXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_OREXPRESSIONNODE_HPP_
#include <Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node represents an OR combination between the two children.
 */
class OrExpressionNode : public LogicalBinaryExpressionNode {
  public:
    OrExpressionNode();
    ~OrExpressionNode() = default;
    /**
    * @brief Create a new OR expression
    */
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_OREXPRESSIONNODE_HPP_
