
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/MulExpressionNode.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
namespace NES {

ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return AddExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return SubExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return DivExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return MulExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator++(ExpressionNodePtr leftExp) {
    return leftExp + ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), "1"));
}

ExpressionNodePtr operator--(ExpressionNodePtr leftExp) {
    return leftExp - ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), "1"));
}

ExpressionNodePtr operator++(ExpressionNodePtr leftExp, int) {
    return leftExp + ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), "1"));
}

ExpressionNodePtr operator--(ExpressionNodePtr leftExp, int) {
    return leftExp - ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), "1"));
}

ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() + rightExp;
}

ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() - rightExp;
}

ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() / rightExp;
}

ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() * rightExp;
}

ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp + rightExp.getExpressionNode();
}

ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp - rightExp.getExpressionNode();
}

ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp / rightExp.getExpressionNode();
}

ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp * rightExp.getExpressionNode();
}

ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() + rightExp.getExpressionNode();
}

ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() - rightExp.getExpressionNode();
}

ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() / rightExp.getExpressionNode();
}

ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() * rightExp.getExpressionNode();
}

ExpressionNodePtr operator++(ExpressionItem exp) {
    return ++exp.getExpressionNode();
}

ExpressionNodePtr operator++(ExpressionItem exp, int) {
    return exp.getExpressionNode()++;
}

ExpressionNodePtr operator--(ExpressionItem exp) {
    return --exp.getExpressionNode();
}

ExpressionNodePtr operator--(ExpressionItem exp, int) {
    return exp.getExpressionNode()--;
}

}// namespace NES