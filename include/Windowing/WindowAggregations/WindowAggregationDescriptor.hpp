#ifndef INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_
#define INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_

#include <Windowing/WindowingForwardRefs.hpp>

namespace NES {

class BinaryOperatorStatement;
class StructDeclaration;
class CompoundStatement;
typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

class FieldAccessExpressionNode;
typedef std::shared_ptr<FieldAccessExpressionNode> FieldAccessExpressionNodePtr;

/**
 * Abstract class for window aggregations. All window aggregations operate on a field and output another field.
 */
class WindowAggregationDescriptor {
  public:
    enum Type{
        AVG,
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
    virtual void compileLiftCombine(CompoundStatementPtr currentCode,
                                    BinaryOperatorStatement expressionStatement,
                                    StructDeclaration inputStruct,
                                    BinaryOperatorStatement inputRef) = 0;

    /**
    * Returns the result field of the aggregation
    * @return
    */
    FieldAccessExpressionNodePtr as();

    /**
    * Returns the result field of the aggregation
    * @return
    */
    FieldAccessExpressionNodePtr on();

    virtual Type getType() = 0;

    /**
     * @brief Infers the stamp of the expression given the current schema.
     * @param SchemaPtr
     */
    virtual void inferStamp(SchemaPtr schema) = 0;

  protected:
    explicit WindowAggregationDescriptor(FieldAccessExpressionNodePtr onField);
    WindowAggregationDescriptor(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField);
    WindowAggregationDescriptor() = default;
    FieldAccessExpressionNodePtr onField;
    FieldAccessExpressionNodePtr asField;
};
}// namespace NES

#endif//INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_
