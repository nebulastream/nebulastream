//
// Created by dbpro4 on 10/23/22.
//


#include </home/dbpro4/nebulastream/nes-runtime/include/Execution/Expressions/Functions/BitcounterExpression.hpp>
#include </home/dbpro4/nebulastream/nes-runtime/include/Nautilus/Interface/FunctionCall.hpp>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

BitcounterExpression::BitcounterExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& SubExpression
                                          )
    : SubExpression(SubExpression){}

double bitcounter (double number){
    return  std::log2(number)+1;
}


Value<> BitcounterExpression::execute(NES::Nautilus::Record& record) const {
    Value leftValue = SubExpression->execute(record);
    Value<> result = FunctionCall<>("bitcounter", bitcounter, leftValue.as<Double>());
    return result;
}



}// namespace NES::Runtime::Execution::Expressions