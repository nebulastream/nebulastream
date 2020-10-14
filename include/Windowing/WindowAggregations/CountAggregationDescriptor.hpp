#ifndef NES_COUNT_HPP
#define NES_COUNT_HPP

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
namespace NES {

/**
 * @brief
 * The CountAggregationDescriptor aggregation calculates the CountAggregationDescriptor over the window.
 */
class CountAggregationDescriptor : public WindowAggregationDescriptor {
  public:
    /**
   * Factory method to creates a CountAggregationDescriptor aggregation on a particular field.
   */
    static WindowAggregationPtr on();

    static WindowAggregationPtr create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField);

    /*
     * @brief generate the code for lift and combine of the CountAggregationDescriptor aggregate
     * @param currentCode
     * @param expressionStatement
     * @param inputStruct
     * @param inputRef
     */
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expressionStatement, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;
    Type getType() override;

    void inferStamp(SchemaPtr schema) override;

  private:
    CountAggregationDescriptor(FieldAccessExpressionNodePtr onField);
    CountAggregationDescriptor(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField);
};
}// namespace NES
#endif//NES_COUNT_HPP
