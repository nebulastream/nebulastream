#ifndef NES_MIN_HPP
#define NES_MIN_HPP

#include <API/Window/WindowAggregation.hpp>
namespace NES {

/**
 * @brief
 * The Min aggregation calculates the minimum over the window.
 */
class Min : public WindowAggregation {
  public:
    /**
   * Factory method to creates a Min aggregation on a particular field.
   */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField) {
        return std::make_shared<Min>(Min(onField, asField));
    }

    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expressionStatement, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;

    template<class InputType, class PartialAggregateType>
    PartialAggregateType lift(InputType inputValue) {
        return inputValue;
    }

    template<class InputType, class PartialAggregateType>
    PartialAggregateType combine(PartialAggregateType partialValue, PartialAggregateType inputValue) {
        if (inputValue < partialValue) {
            partialValue = inputValue;
        }
        return partialValue;
    }

    template<class InputType, class FinalAggregateType>
    FinalAggregateType lower(InputType inputValue) {
        return inputValue;
    }
  private:
    Min(NES::AttributeFieldPtr onField);
    Min(AttributeFieldPtr onField, AttributeFieldPtr asField);
};
}// namespace NES
#endif//NES_MIN_HPP
