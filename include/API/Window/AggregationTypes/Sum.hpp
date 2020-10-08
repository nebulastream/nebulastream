#ifndef NES_SUM_HPP
#define NES_SUM_HPP

#include <API/Window/WindowAggregation.hpp>
namespace NES {
/**
 * @brief
 * The Sum aggregation calculates the running sum over the window.
 */
class Sum : public WindowAggregation {
  public:
    /**
   * Factory method to creates a sum aggregation on a particular field.
   */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField) {
        return std::make_shared<Sum>(Sum(onField, asField));
    }
    void compileLiftCombine(CompoundStatementPtr currentCode,
                            BinaryOperatorStatement partialRef,
                            StructDeclaration inputStruct,
                            BinaryOperatorStatement inputRef);

    template<class InputType, class PartialAggregateType>
    PartialAggregateType lift(InputType inputValue) {
        return inputValue;
    }

    template<class InputType, class PartialAggregateType>
    PartialAggregateType combine(PartialAggregateType partialValue, InputType inputValue) {
        return partialValue + inputValue;
    }

    template<class InputType, class FinalAggregateType>
    FinalAggregateType lower(InputType inputValue) {
        return inputValue;
    }

  private:
    Sum(NES::AttributeFieldPtr onField);
    Sum(AttributeFieldPtr onField, AttributeFieldPtr asField);
};
}// namespace NES
#endif//NES_SUM_HPP
