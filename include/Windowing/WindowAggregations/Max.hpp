#ifndef NES_MAX_HPP
#define NES_MAX_HPP

#include <Windowing/WindowAggregations/WindowAggregation.hpp>
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

    static WindowAggregationPtr create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField);
    /*
     * @brief generate the code for lift and combine of the Max aggregate
     * @param currentCode
     * @param expressionStatement
     * @param inputStruct
     * @param inputRef
     */
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expression_statment, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;

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
    PartialAggregateType combine(PartialAggregateType partialValue, PartialAggregateType inputValue) {
        if (inputValue > partialValue) {
            partialValue = inputValue;
        }
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
    Max(NES::AttributeFieldPtr onField);
    Max(AttributeFieldPtr onField, AttributeFieldPtr asField);
};
}// namespace NES
#endif//NES_MAX_HPP
