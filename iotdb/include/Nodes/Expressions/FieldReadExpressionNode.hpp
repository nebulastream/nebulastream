#ifndef NES_INCLUDE_NODES_EXPRESSIONS_FIELDREADEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_FIELDREADEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
class FieldReadExpressionNode : public ExpressionNode {
  public:
    static ExpressionNodePtr create(DataTypePtr stamp, std::string fieldName);
    FieldReadExpressionNode(DataTypePtr stamp, std::string fieldName);
    FieldReadExpressionNode(std::string fieldName);
    const std::string toString() const override;
    bool equal(const NodePtr rhs) const override;
  private:
    std::string fieldName;
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_FIELDREADEXPRESSIONNODE_HPP_
