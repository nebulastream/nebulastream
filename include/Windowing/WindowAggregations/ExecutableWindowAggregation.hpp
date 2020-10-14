#ifndef NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEWINDOWAGGREGATION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEWINDOWAGGREGATION_HPP_
#include <Windowing/WindowingForwardRefs.hpp>
#include <threads.h>
namespace NES{

class BinaryOperatorStatement;
class StructDeclaration;
class CompoundStatement;
typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

template<class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableWindowAggregation{

  public:

    virtual PartialAggregateType lift(InputType input)  = 0;

    virtual PartialAggregateType combine(PartialAggregateType partialAggregate1, PartialAggregateType partialAggregate2) = 0;

    virtual FinalAggregateType lower(PartialAggregateType partialAggregate) = 0;

};

}

#endif//NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEWINDOWAGGREGATION_HPP_
