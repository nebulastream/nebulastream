#ifndef NES_INCLUDE_API_EXPRESSIONS_LOGICALEXPRESSIONS_HPP_
#define NES_INCLUDE_API_EXPRESSIONS_LOGICALEXPRESSIONS_HPP_

#include <memory>
namespace NES {

class ExpressionNode;

class ExpressionItem;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

/**
 * @brief Defines common logical operations between expression nodes.
 */
ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator!(ExpressionNodePtr exp);

/**
 * @brief Defines common operations between a constant and an expression node.
 */
ExpressionNodePtr operator&&(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator||(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator==(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator!=(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator<=(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator>=(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator<(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator>(ExpressionItem leftExp, ExpressionNodePtr rightExp);

/**
 * @brief Defines common logical operations between an expression node and a constant.
 */
ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionItem rightExp);

/**
 * @brief Defines common logical operations between two expression items.
 */
ExpressionNodePtr operator&&(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator||(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator==(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator!=(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator<=(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator>=(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator<(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator>(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator!(ExpressionItem exp);

}// namespace NES
#endif// NES_INCLUDE_API_EXPRESSIONS_LOGICALEXPRESSIONS_HPP_
