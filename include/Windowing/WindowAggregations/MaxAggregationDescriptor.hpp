#ifndef NES_MAX_HPP
#define NES_MAX_HPP

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
namespace NES::Windowing {

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

    static WindowAggregationPtr create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField);
    /*
     * @brief generate the code for lift and combine of the MaxAggregationDescriptor aggregate
     * @param currentCode
     * @param expressionStatement
     * @param inputStruct
     * @param inputRef
     */
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expression_statment, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override;
    DataTypePtr getInputStamp() override;
    DataTypePtr getPartialAggregateStamp() override;
    DataTypePtr getFinalAggregateStamp() override;

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
    MaxAggregationDescriptor(ExpressionNodePtr onField, ExpressionNodePtr asField);

  private:
    MaxAggregationDescriptor(FieldAccessExpressionNodePtr onField);
};
}// namespace NES::Windowing
#endif//NES_MAX_HPP
