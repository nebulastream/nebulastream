#ifndef NES_MAX_HPP
#define NES_MAX_HPP

#include <API/Window/WindowAggregation.hpp>
namespace NES {

/**
 * @brief
 * The Max aggregation calculates the maximum over the window.
 */
class Max : public WindowAggregation {
  public:
    /**
   * Factory method to creates a Max aggregation on a particular field.
   */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField) {
        return std::make_shared<Max>(Max(onField, asField));
    }

    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expression_statment, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;

    template<class InputType, class PartialAggregateType>
    PartialAggregateType lift(InputType inputValue) {
        return inputValue;
    }

    template<class InputType, class PartialAggregateType>
    PartialAggregateType combine(PartialAggregateType partialValue, PartialAggregateType inputValue) {
        if (inputValue > partialValue) {
            partialValue = inputValue;
        }
        return partialValue;
    }

    template<class InputType, class FinalAggregateType>
    FinalAggregateType lower(InputType inputValue) {
        return inputValue;
    }
  private:
    Max(NES::AttributeFieldPtr onField);
    Max(AttributeFieldPtr onField, AttributeFieldPtr asField);
};
}// namespace NES
#endif//NES_MAX_HPP
