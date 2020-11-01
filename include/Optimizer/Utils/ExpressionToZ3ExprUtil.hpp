#ifndef NES_OPTIMIZE_UTILS_EXPRESSIONTOZ3EXPRUTIL_HPP
#define NES_OPTIMIZE_UTILS_EXPRESSIONTOZ3EXPRUTIL_HPP

#include <memory>

namespace z3 {
class expr;
class context;
}// namespace z3

namespace NES {

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

/**
 * @brief This class is responsible for taking input as a logical expression and generating an equivalent Z3 expression.
 */
class ExpressionToZ3ExprUtil {

  public:

    /**
     * @brief Convert input expression into an equivalent Z3 expressions
     * @param expression: the input expression
     * @param context: Z3 context
     * @return returns object representing Z3 expression
     */
    static z3::expr createForExpression(ExpressionNodePtr expression, z3::context &context);

  private:

    /**
     * @brief Convert input Logical expression into an equivalent Z3 expression
     * @param expression: the input logical expression
     * @param context: the Z3 context
     * @return returns object representing Z3 expression
     */
    static z3::expr createForLogicalExpressions(ExpressionNodePtr expression, z3::context &context);

    /**
     * @brief Convert input arithmetic expression into an equivalent Z3 expression
     * @param expression: the input arithmetic expression
     * @param context: the Z3 context
     * @return returns object representing Z3 expression
     */
    static z3::expr createForArithmeticalExpressions(ExpressionNodePtr expression, z3::context &context);
};
}// namespace NES

#endif//NES_OPTIMIZE_UTILS_EXPRESSIONTOZ3EXPRUTIL_HPP
