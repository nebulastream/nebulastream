#ifndef NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
namespace NES{
/**
 * @brief This node represents a arithmetical expression.
 */
class ArithmeticalExpressionNode : public BinaryExpressionNode {
  public:
    /**
     * @brief Infers the stamp of this arithmetical expression node.
     * Currently the type inference is equal for all arithmetical expression and expects numerical data types as operands.
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    bool equal(NodePtr rhs) const override;
    const std::string toString() const override;

  protected:
    ArithmeticalExpressionNode(DataTypePtr stamp);
    ~ArithmeticalExpressionNode() = default;
};

}

#endif // NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
