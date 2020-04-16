#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>

namespace NES {

ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return OrExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return AndExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return EqualsExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return NegateExpressionNode::create(EqualsExpressionNode::create(leftExp, rightExp));
}

ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return LessEqualsExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return LessExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return GreaterEqualsExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return GreaterExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator!(ExpressionNodePtr exp) {
    return NegateExpressionNode::create(exp);
}


ExpressionNodePtr operator||(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() || rightExp;
}

ExpressionNodePtr operator&&(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() && rightExp;
}

ExpressionNodePtr operator==(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() == rightExp;
}

ExpressionNodePtr operator!=(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() != rightExp;
}

ExpressionNodePtr operator<=(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() <= rightExp;
}

ExpressionNodePtr operator<(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() < rightExp;
}

ExpressionNodePtr operator>=(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() >= rightExp;
}

ExpressionNodePtr operator>(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() > rightExp;
}

ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp || rightExp.getExpressionNode();
}

ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp && rightExp.getExpressionNode();
}

ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp == rightExp.getExpressionNode();
}

ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp != rightExp.getExpressionNode();
}

ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp <= rightExp.getExpressionNode();
}

ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp < rightExp.getExpressionNode();
}

ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp >= rightExp.getExpressionNode();
}

ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp > rightExp.getExpressionNode();
}

ExpressionNodePtr operator||(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() || rightExp.getExpressionNode();
}

ExpressionNodePtr operator&&(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() && rightExp.getExpressionNode();
}

ExpressionNodePtr operator==(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() == rightExp.getExpressionNode();
}

ExpressionNodePtr operator!=(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() != rightExp.getExpressionNode();
}

ExpressionNodePtr operator<=(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() <= rightExp.getExpressionNode();
}

ExpressionNodePtr operator<(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() < rightExp.getExpressionNode();
}

ExpressionNodePtr operator>=(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() >= rightExp.getExpressionNode();
}

ExpressionNodePtr operator>(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() > rightExp.getExpressionNode();
}

ExpressionNodePtr operator!(ExpressionItem leftExp) {
    return !leftExp.getExpressionNode();
}



}