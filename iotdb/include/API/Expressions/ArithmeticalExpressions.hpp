#ifndef NES_INCLUDE_API_EXPRESSIONS_ARITHMETICALEXPRESSIONS_HPP_
#define NES_INCLUDE_API_EXPRESSIONS_ARITHMETICALEXPRESSIONS_HPP_
#include <memory>
namespace NES{

class ExpressionNode;

class ExpressionItem;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

/**
 * @brief Defines common arithmetical operations between expression nodes.
 */
ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator--(ExpressionNodePtr exp);
ExpressionNodePtr operator++(ExpressionNodePtr exp);
ExpressionNodePtr operator++(ExpressionNodePtr exp, int value);
ExpressionNodePtr operator--(ExpressionNodePtr exp, int value);

/**
 * @brief Defines common arithmetical operations between a constant and an expression node.
 */
ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionNodePtr rightExp);

/**
 * @brief Defines common arithmetical operations between an expression node and a constant.
 */
ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionItem rightExp);

/**
 * @brief Defines common arithmetical operations between two expression items.
 */
ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator++(ExpressionItem exp);
ExpressionNodePtr operator--(ExpressionItem exp);
ExpressionNodePtr operator++(ExpressionItem exp, int);
ExpressionNodePtr operator--(ExpressionItem exp, int);

}
#endif // NES_INCLUDE_API_EXPRESSIONS_ARITHMETICALEXPRESSIONS_HPP_
