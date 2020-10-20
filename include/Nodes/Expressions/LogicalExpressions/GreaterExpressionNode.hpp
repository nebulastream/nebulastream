#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_GREATEREXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_GREATEREXPRESSIONNODE_HPP_
#include <Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node represents a greater comparision between the two children.
 */
class GreaterExpressionNode : public LogicalBinaryExpressionNode {
  public:
    GreaterExpressionNode();
    ~GreaterExpressionNode() = default;
    /**
    * @brief Create a new greater expression
    */
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    ExpressionNodePtr copy() override;
};
}// namespace NES

#endif//NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_GREATEREXPRESSIONNODE_HPP_
