#ifndef INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_
#define INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_

#include <Windowing/WindowingForwardRefs.hpp>

namespace NES {
class BinaryOperatorStatement;
class StructDeclaration;
class CompoundStatement;
typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

}// namespace NES

namespace NES::Windowing {

/**
 * Abstract class for window aggregations. All window aggregations operate on a field and output another field.
 */
class WindowAggregationDescriptor {
  public:
    enum Type {
        Avg,
        Count,
        Max,
        Min,
        Sum
    };

    /**
    * Defines the field to which a aggregate output is assigned.
    * @param asField
    * @return WindowAggregationDescriptor
    */
    WindowAggregationDescriptor& as(FieldAccessExpressionNodePtr asField);

    /**
   * Generates code for the particular window aggregate.
   * TODO in a later version we will hide this in the corresponding physical operator.
   */
    virtual void compileLiftCombine(NES::CompoundStatementPtr currentCode,
                                    NES::BinaryOperatorStatement expressionStatement,
                                    NES::StructDeclaration inputStruct,
                                    NES::BinaryOperatorStatement inputRef) = 0;

    /**
    * Returns the result field of the aggregation
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr as();

    /**
    * Returns the result field of the aggregation
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr on();

    /**
     * @brief Returns the type of this aggregation.
     * @return WindowAggregationDescriptor::Type
     */
    virtual Type getType() = 0;

    /**
     * @brief Infers the stamp of the expression given the current schema.
     * @param SchemaPtr
     */
    virtual void inferStamp(SchemaPtr schema) = 0;

    /**
    * @brief Creates a deep copy of the window aggregation
    */
    virtual WindowAggregationPtr copy() = 0;

    /**
     * @return the input type
     */
    virtual DataTypePtr getInputStamp() = 0;

    /**
     * @return the partial aggregation type
     */
    virtual DataTypePtr getPartialAggregateStamp() = 0;

    /**
     * @return the final aggregation type
     */
    virtual DataTypePtr getFinalAggregateStamp() = 0;

  protected:
    explicit WindowAggregationDescriptor(FieldAccessExpressionNodePtr onField);
    WindowAggregationDescriptor(ExpressionNodePtr onField, ExpressionNodePtr asField);
    WindowAggregationDescriptor() = default;
    ExpressionNodePtr onField;
    ExpressionNodePtr asField;
};
}// namespace NES::Windowing

#endif//INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_
