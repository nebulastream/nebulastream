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
    /**
    * Generates code for the particular window aggregate.
    */
    virtual void compileLiftCombine(CompoundStatementPtr currentCode,
                                    BinaryOperatorStatement expressionStatement,
                                    StructDeclaration inputStruct,
                                    BinaryOperatorStatement inputRef) = 0;

    virtual PartialAggregateType lift(InputType input)  = 0;

    virtual PartialAggregateType combine(PartialAggregateType partialAggregate1, PartialAggregateType partialAggregate2) = 0;

    virtual FinalAggregateType lower(PartialAggregateType partialAggregate) = 0;

    /**
    * Returns the result field of the aggregation
    * @return
    */
    AttributeFieldPtr as(){
        return asField;
    };

    /**
    * Returns the result field of the aggregation
    * @return
    */
    AttributeFieldPtr on(){
        return onField;
    }

  protected:
    ExecutableWindowAggregation(AttributeFieldPtr onField, AttributeFieldPtr asField): onField(onField), asField(asField){};
    const AttributeFieldPtr onField;
    AttributeFieldPtr asField;
};

}

#endif//NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEWINDOWAGGREGATION_HPP_
