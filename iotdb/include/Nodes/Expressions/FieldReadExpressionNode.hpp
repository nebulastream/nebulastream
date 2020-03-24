#ifndef NES_INCLUDE_NODES_EXPRESSIONS_FIELDREADEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_FIELDREADEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief A FieldReadExpression reads a specific field of the current record.
 * It can be created typed or untyped.
 * todo for untyped field accesses we have to infer the type later.
 */
class FieldReadExpressionNode : public ExpressionNode {
  public:
    FieldReadExpressionNode(DataTypePtr stamp, std::string fieldName);
    FieldReadExpressionNode(std::string fieldName);

    /**
    * @brief Create typed field read.
    */
    static ExpressionNodePtr create(DataTypePtr stamp, std::string fieldName);

    /**
     * @brief Create untyped field read.
     */
    static ExpressionNodePtr create(std::string fieldName);

    const std::string toString() const override;
    bool equal(const NodePtr rhs) const override;

  private:
    /**
     * @brief Name of the field want to access.
     */
    std::string fieldName;
};

}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_FIELDREADEXPRESSIONNODE_HPP_
