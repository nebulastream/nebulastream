//
// Created by dbpro4 on 10/23/22.
//

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_BITCOUNTEREXPRESSION_HPP
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_BITCOUNTEREXPRESSION_HPP

#include </home/dbpro4/nebulastream/nes-runtime/include/Execution/Expressions/Expression.hpp>
#include </home/dbpro4/nebulastream/nes-runtime/include/Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Runtime::Execution::Expressions {

/**
 * @brief This expression mod the leftSubExpression with the rightSubExpression.
 */
class BitcounterExpression : public Expression {
  public:
    BitcounterExpression(const ExpressionPtr& SubExpression);
    Value<> execute(NES::Nautilus::Record& record) const override;

    private:
      const  ExpressionPtr SubExpression;

};

}// namespace NES::Runtime::Execution::Expressions


#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_BITCOUNTEREXPRESSION_HPP

