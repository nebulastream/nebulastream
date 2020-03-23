#ifndef NES_INCLUDE_NODES_EXPRESSIONS_FIELDREADEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_FIELDREADEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
class FieldReadExpressionNode : public ExpressionNode {
    FieldReadExpressionNode(DataTypePtr stamp, std::string fieldName);
    FieldReadExpressionNode(std::string fieldName);
  private:
    std::string fieldName;
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_FIELDREADEXPRESSIONNODE_HPP_
