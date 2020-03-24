#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
class UnaryExpressionNode : public ExpressionNode {
  protected:
    UnaryExpressionNode(DataTypePtr stamp);

    void setChild(ExpressionNodePtr child){
        this->addChild(child);
    }

    ExpressionNodePtr child() const {
        return children[0]->as<ExpressionNode>();
    }
};
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONNODE_HPP_
