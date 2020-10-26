#ifndef NES_COUNT_HPP
#define NES_COUNT_HPP

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
namespace NES::Windowing {

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

    /**
     * @brief Returns the type of this aggregation.
     * @return WindowAggregationDescriptor::Type
     */
    Type getType() override;

    /**
     * @brief Infers the stamp of the expression given the current schema.
     * @param SchemaPtr
     */
    void inferStamp(SchemaPtr schema) override;

    WindowAggregationPtr copy() override;

  private:
    CountAggregationDescriptor(FieldAccessExpressionNodePtr onField);
    CountAggregationDescriptor(ExpressionNodePtr onField, ExpressionNodePtr asField);
};
}// namespace NES
#endif//NES_COUNT_HPP
