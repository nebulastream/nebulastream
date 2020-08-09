#ifndef INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_
#define INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_
#include <API/AbstractWindowDefinition.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/AttributeField.hpp>
#include <API/UserAPIExpression.hpp>

namespace NES {

class BinaryOperatorStatement;
class StructDeclaration;
class CompoundStatement;
typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

/**
 * Abstract class for window aggregations. All window aggregations operate on a field and output another field.
 */
class WindowAggregation {
  public:
    /**
   * Defines the field to which a aggregate output is assigned.
   * @param asField
   * @return WindowAggregation
   */
    WindowAggregation& as(const AttributeFieldPtr asField);

    /**
   * Generates code for the particular window aggregate.
   * TODO in a later version we will hide this in the corresponding physical operator.
   */
    virtual void compileLiftCombine(CompoundStatementPtr currentCode,
                                    BinaryOperatorStatement expression_statment,
                                    StructDeclaration inputStruct,
                                    BinaryOperatorStatement inputRef) = 0;

    /**
   * Returns the result field of the aggregation
   * @return
   */
    AttributeFieldPtr as() {
        if (asField == nullptr)
            return onField;
        return asField;
    }

    /**
  * Returns the result field of the aggregation
  * @return
  */
    AttributeFieldPtr on() {
        return onField;
    }

  protected:
    WindowAggregation(const AttributeFieldPtr onField);
    WindowAggregation(const AttributeFieldPtr onField, const AttributeFieldPtr asField);
    WindowAggregation() = default;
    const AttributeFieldPtr onField;
    AttributeFieldPtr asField;
};

/**
 * The Sum aggregation calculates the running sum over the window.
 */
class Sum : public WindowAggregation {
  public:
    /**
   * Factory method to creates a sum aggregation on a particular field.
   */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField){
        return std::make_shared<Sum>(Sum(onField, asField));
    }
    void compileLiftCombine(CompoundStatementPtr currentCode,
                            BinaryOperatorStatement partialRef,
                            StructDeclaration inputStruct,
                            BinaryOperatorStatement inputRef);

    template<class InputType, class PartialAggregateType>
    PartialAggregateType lift(InputType input) {
        auto input_type = this->onField->getDataType();
        return input;
    }

    template<class InputType, class PartialAggregateType>
    PartialAggregateType combine(PartialAggregateType partialType, InputType input) {
        return partialType + input;
    }

    template<class InputType, class FinalAggregateType>
    FinalAggregateType lower(InputType partialType) {
        return partialType;
    }

  private:
    Sum(NES::AttributeFieldPtr onField);
    Sum(AttributeFieldPtr onField, AttributeFieldPtr asField);
};
}// namespace NES

#endif//INCLUDE_API_WINDOW_WINDOWAGGREGATION_HPP_
