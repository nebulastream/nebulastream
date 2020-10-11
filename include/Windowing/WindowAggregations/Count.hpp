#ifndef NES_COUNT_HPP
#define NES_COUNT_HPP

#include <Windowing/WindowAggregations/WindowAggregation.hpp>
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
    /*
     * @brief generate the code for lift and combine of the Count aggregate
     * @param currentCode
     * @param expressionStatement
     * @param inputStruct
     * @param inputRef
     */
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expressionStatement, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;

    /*
     * @brief maps the input element to an element PartialAggregateType
     * @param input value of the element
     * @return the element that mapped to PartialAggregateType
     */
    template<class InputType, class PartialAggregateType>
    PartialAggregateType lift(InputType inputValue) {
        return inputValue;
    }

    /*
     * @brief combines two partial aggregates to a new partial aggregate
     * @param current partial value
     * @param the new input element
     * @return new partial aggregate as combination of partialValue and inputValue
     */
    template<class InputType, class PartialAggregateType>
    PartialAggregateType combine(PartialAggregateType partialValue, PartialAggregateType) {
        ++partialValue;
        return partialValue;
    }
    /*
     * @brief maps partial aggregates to an element of FinalAggregationType
     * @param partial aggregate element
     * @return element mapped to FinalAggregationType
     */
    template<class PartialAggregateType, class FinalAggregateType>
    FinalAggregateType lower(PartialAggregateType partialAggregateValue) {
        return partialAggregateValue;
    }

  private:
    Count(NES::AttributeFieldPtr onField);
    Count(AttributeFieldPtr onField, AttributeFieldPtr asField);
};
}// namespace NES
#endif//NES_COUNT_HPP
