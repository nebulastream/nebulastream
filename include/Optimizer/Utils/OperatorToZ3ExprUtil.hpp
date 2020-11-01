#ifndef NES_OPTIMIZE_UTILS_OPERATORTOZ3EXPRUTIL_HPP
#define NES_OPTIMIZE_UTILS_OPERATORTOZ3EXPRUTIL_HPP

#include <memory>

namespace z3 {
class expr;
class context;
}// namespace z3

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

/**
 * @brief This class is responsible for creating the Z3 expression for input operators
 */
class OperatorToZ3ExprUtil {
  public:
    /**
     * @brief Convert input operator into an equivalent logical expression
     * @param operatorNode: the input operator
     * @param context: the context of Z3
     * @return the object representing Z3 expression
     */
    static z3::expr createForOperator(OperatorNodePtr operatorNode, z3::context& context);
};
}// namespace NES

#endif//NES_OPTIMIZE_UTILS_OPERATORTOZ3EXPRUTIL_HPP
