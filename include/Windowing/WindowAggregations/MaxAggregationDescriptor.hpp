#ifndef NES_MAX_HPP
#define NES_MAX_HPP

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
namespace NES {

/**
 * @brief
 * The MaxAggregationDescriptor aggregation calculates the maximum over the window.
 */
class MaxAggregationDescriptor : public WindowAggregationDescriptor {
  public:
    /**
   * Factory method to creates a MaxAggregationDescriptor aggregation on a particular field.
   */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField);
    /*
     * @brief generate the code for lift and combine of the MaxAggregationDescriptor aggregate
     * @param currentCode
     * @param expressionStatement
     * @param inputStruct
     * @param inputRef
     */
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expression_statment, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;

    Type getType() override;

  private:
    MaxAggregationDescriptor(NES::AttributeFieldPtr onField);
    MaxAggregationDescriptor(AttributeFieldPtr onField, AttributeFieldPtr asField);
};
}// namespace NES
#endif//NES_MAX_HPP
