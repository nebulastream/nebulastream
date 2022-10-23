//
// Created by dbpro1 on 10/21/22.
//

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_MODEXPRESSION_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_MODEXPRESSION_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Runtime::Execution::Expressions {

/**
 * @brief This expression mod the leftSubExpression with the rightSubExpression.
 */
class CosExpression : public Expression {
  public:
    CosExpression(const ExpressionPtr& leftSubExpression);
    Value<> execute(Record& record) const override;

  private:
    const  ExpressionPtr leftSubExpression;
};

}// namespace NES::Runtime::Execution::Expressions


#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_MODEXPRESSION_HPP_
