#ifndef INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_
#define INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_

#include <Windowing/WindowingForwardRefs.hpp>

namespace NES {

class BinaryOperatorStatement;
class StructDeclaration;
class CompoundStatement;
typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

/**
 * Abstract class for window aggregations. All window aggregations operate on a field and output another field.
 */
class WindowAggregationDescriptor {
  public:
    /**
    * Defines the field to which a aggregate output is assigned.
   * @param asField
    * @return WindowAggregationDescriptor
    */
    WindowAggregationDescriptor& as(AttributeFieldPtr asField);

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
    AttributeFieldPtr as();

    /**
    * Returns the result field of the aggregation
    * @return
    */
    AttributeFieldPtr on();

  protected:
    explicit WindowAggregationDescriptor(AttributeFieldPtr onField);
    WindowAggregationDescriptor(AttributeFieldPtr onField, AttributeFieldPtr asField);
    WindowAggregationDescriptor() = default;
    const AttributeFieldPtr onField;
    AttributeFieldPtr asField;
};
}// namespace NES

#endif//INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_
