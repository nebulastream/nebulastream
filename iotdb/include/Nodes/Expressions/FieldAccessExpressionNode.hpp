#ifndef NES_INCLUDE_NODES_EXPRESSIONS_FIELDACCESSEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_FIELDACCESSEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief A FieldAccessExpression reads a specific field of the current record.
 * It can be created typed or untyped.
 */
class FieldAccessExpressionNode : public ExpressionNode {
  public:
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

    const std::string getFieldName();

    /**
    * @brief Infers the stamp of the expression given the current schema.
    * @param SchemaPtr
    */
    void inferStamp(SchemaPtr schema) override;

  private:
    FieldAccessExpressionNode(DataTypePtr stamp, std::string fieldName);
    /**
     * @brief Name of the field want to access.
     */
    std::string fieldName;
};

typedef std::shared_ptr<FieldAccessExpressionNode> FieldAccessExpressionNodePtr;

}

#endif // NES_INCLUDE_NODES_EXPRESSIONS_FIELDACCESSEXPRESSIONNODE_HPP_
