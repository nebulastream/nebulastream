#ifndef NES_OPTIMIZE_UTILS_EXPRESSIONTOFOLUTIL_HPP
#define NES_OPTIMIZE_UTILS_EXPRESSIONTOFOLUTIL_HPP

#include <memory>

namespace z3 {
class expr;
class context;
}// namespace z3

namespace NES {

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

/**
 * @brief This class is responsible for taking input as a logical expression and generating output as a Z3::expr representing an
 * equivalent First Order Logic expression.
 */
class ExpressionToFOLUtil {

  public:
    /**
    * @brief Convert a expression node and all its children to a Z3::expression object.
    * @param expression The root expression node to serialize.
    * @return the z3::expresion object
    */
    static z3::expr serializeExpression(ExpressionNodePtr expression, z3::context &context);

  private:
    static z3::expr serializeLogicalExpressions(ExpressionNodePtr sharedPtr, z3::context &context);
    static z3::expr serializeArithmeticalExpressions(ExpressionNodePtr expression, z3::context &context);
};
}// namespace NES

#endif//NES_OPTIMIZE_UTILS_EXPRESSIONTOFOLUTIL_HPP
