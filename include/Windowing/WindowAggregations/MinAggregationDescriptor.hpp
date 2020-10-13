#ifndef NES_MIN_HPP
#define NES_MIN_HPP

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
namespace NES {

/**
 * @brief
 * The MinAggregationDescriptor aggregation calculates the minimum over the window.
 */
class MinAggregationDescriptor : public WindowAggregationDescriptor {
  public:
    /**
   * Factory method to creates a MinAggregationDescriptor aggregation on a particular field.
   */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField);

    /*
     * @brief generate the code for lift and combine of MinAggregationDescriptor SumAggregationDescriptor aggregate
     * @param currentCode
     * @param expressionStatement
     * @param inputStruct
     * @param inputRef
     */
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expressionStatement, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;

    Type getType() override;

  private:
    MinAggregationDescriptor(NES::AttributeFieldPtr onField);
    MinAggregationDescriptor(AttributeFieldPtr onField, AttributeFieldPtr asField);
};
}// namespace NES
#endif//NES_MIN_HPP
