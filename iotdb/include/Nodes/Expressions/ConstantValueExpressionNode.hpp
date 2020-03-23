#ifndef NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
class ConstantValueExpressionNode : public ExpressionNode {
    ConstantValueExpressionNode(ConstantValuePtr value);

};

ExpressionNodePtr createConstValueExpressionNode();
}

#endif //NES_INCLUDE_NODES_EXPRESSIONS_CONSTANTVALUEEXPRESSIONNODE_HPP_
