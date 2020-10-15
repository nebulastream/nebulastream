#ifndef NES_SUM_HPP
#define NES_SUM_HPP

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
namespace NES {
/**
 * @brief
 * The SumAggregationDescriptor aggregation calculates the running sum over the window.
 */
class SumAggregationDescriptor : public WindowAggregationDescriptor {
  public:
    /**
   * Factory method to creates a sum aggregation on a particular field.
   */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField);

    /*
     * @brief generate the code for lift and combine of the SumAggregationDescriptor aggregate
     * @param currentCode
     * @param expressionStatement
     * @param inputStruct
     * @param inputRef
     */
    void compileLiftCombine(CompoundStatementPtr currentCode,
                            BinaryOperatorStatement partialRef,
                            StructDeclaration inputStruct,
                            BinaryOperatorStatement inputRef) override;


    /**
     * @brief Returns the type of this aggregation.
     * @return WindowAggregationDescriptor::Type
     */
    Type getType() override;

    /**
    * @brief Infers the stamp of the expression given the current schema.
    *  @param SchemaPtr
   */
    void inferStamp(SchemaPtr schema) override;

  private:
    SumAggregationDescriptor(FieldAccessExpressionNodePtr onField);
    SumAggregationDescriptor(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField);
};
}// namespace NES
#endif//NES_SUM_HPP
