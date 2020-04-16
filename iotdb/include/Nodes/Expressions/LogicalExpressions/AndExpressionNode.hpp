#ifndef NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_ANDEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_ANDEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/LogicalExpressions/LogicalBinaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node represents an AND combination between the two children.
 */
class AndExpressionNode : public LogicalBinaryExpressionNode {
  public:
    AndExpressionNode();
    ~AndExpressionNode() = default;
    /**
    * @brief Create a new AND expression
    */
    static ExpressionNodePtr create(const ExpressionNodePtr left, const ExpressionNodePtr right);
    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_BINARYEXPRESSIONS_ANDEXPRESSIONNODE_HPP_
