#ifndef NES_INCLUDE_API_EXPRESSIONS_ARITHMETICALEXPRESSIONS_HPP_
#define NES_INCLUDE_API_EXPRESSIONS_ARITHMETICALEXPRESSIONS_HPP_
namespace NES{

class ExpressionNode;

class ExpressionItem;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

/**
 * @brief Defines common operations between expression nodes.
 */
ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator--(ExpressionNodePtr exp);
ExpressionNodePtr operator++(ExpressionNodePtr exp);

/**
 * @brief Defines common operations between a constant and an expression node.
 */
ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionNodePtr rightExp);

/**
 * @brief Defines common operations between an expression node and a constant.
 */
ExpressionNodePtr operator-(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator+(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator*(ExpressionNodePtr leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator/(ExpressionNodePtr leftExp, ExpressionItem rightExp);

/**
 * @brief Defines common operations between two constants.
 */
ExpressionNodePtr operator-(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator+(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator*(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator/(ExpressionItem leftExp, ExpressionItem rightExp);
ExpressionNodePtr operator++(ExpressionItem exp);
ExpressionNodePtr operator--(ExpressionItem exp);

}
#endif // NES_INCLUDE_API_EXPRESSIONS_ARITHMETICALEXPRESSIONS_HPP_
