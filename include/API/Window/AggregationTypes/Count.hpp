#ifndef NES_COUNT_HPP
#define NES_COUNT_HPP

#include <API/Window/WindowAggregation.hpp>
namespace NES {

/**
 * @brief
 * The Count aggregation calculates the Count over the window.
 */
class Count : public WindowAggregation {
  public:
    /**
   * Factory method to creates a Count aggregation on a particular field.
   */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField) {
        return std::make_shared<Count>(Count(onField, asField));
    }

    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expression_statment, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;

    template<class InputType, class PartialAggregateType>
    PartialAggregateType lift(InputType inputValue) {
        return inputValue;
    }

    template<class InputType, class PartialAggregateType>
    PartialAggregateType combine(PartialAggregateType partialValue, PartialAggregateType inputValue) {
        ++partialValue;
        return partialValue;
    }

    template<class InputType, class FinalAggregateType>
    FinalAggregateType lower(InputType inputValue) {
        return inputValue;
    }
  private:
    Count(NES::AttributeFieldPtr onField);
    Count(AttributeFieldPtr onField, AttributeFieldPtr asField);
};
}// namespace NES
#endif//NES_COUNT_HPP
