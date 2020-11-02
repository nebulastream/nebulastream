#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_NEGATEEXPRESSINNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_NEGATEEXPRESSINNODE_HPP_
#include <Nodes/Expressions/LogicalExpressions/LogicalUnaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node negates its child expression.
 */
class NegateExpressionNode : public LogicalUnaryExpressionNode {
  public:
    NegateExpressionNode();
    ~NegateExpressionNode() = default;

    /**
     * @brief Create a new negate expression
     */
    static ExpressionNodePtr create(const ExpressionNodePtr child);

    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    /**
     * @brief Infers the stamp of this logical negate expression node.
     * We assume that the children of this expression is a predicate.
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit NegateExpressionNode(NegateExpressionNode* other);
};
}// namespace NES

#endif//NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_NEGATEEXPRESSINNODE_HPP_
