#ifndef NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEAVGAGGREGATION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEAVGAGGREGATION_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableWindowAggregation.hpp>
#include <memory>
#include <utility>
namespace NES {

template<typename SumType>
class AVGPartialType {
  public:
    explicit AVGPartialType(SumType sum) : sum(sum), count(1) {}
    SumType sum;
    int64_t count;
};

typedef double AVGResultType;

template<typename InputType, std::enable_if_t<std::is_integral<InputType>::value> = 0>
class ExecutableAVGAggregation : public ExecutableWindowAggregation<InputType, AVGPartialType<InputType>, AVGResultType> {
  public:
    ExecutableAVGAggregation(AttributeFieldPtr onField, AttributeFieldPtr asField) : ExecutableWindowAggregation<InputType, AVGPartialType<InputType>, AVGResultType>(onField, asField){};

    static std::shared_ptr<ExecutableWindowAggregation<InputType, AVGPartialType<InputType>, AVGResultType>> create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField) {
        return std::make_shared<ExecutableAVGAggregation<InputType>>(std::move(onField), std::move(asField));
    };

    /*
    * @brief generate the code for lift and combine of the MaxAggregationDescriptor aggregate
    * @param currentCode
    * @param expressionStatement
    * @param inputStruct
    * @param inputRef
    */
    void compileLiftCombine(CompoundStatementPtr, BinaryOperatorStatement, StructDeclaration, BinaryOperatorStatement) override {
       NES_NOT_IMPLEMENTED();
    };

    /*
     * @brief maps the input element to an element PartialAggregateType
     * @param input value of the element
     * @return the element that mapped to PartialAggregateType
     */
    AVGPartialType<InputType> lift(InputType inputValue) override {
        return AVGPartialType<InputType>(inputValue);
    }

    /*
     * @brief combines two partial aggregates to a new partial aggregate
     * @param current partial value
     * @param the new input element
     * @return new partial aggregate as combination of partialValue and inputValue
     */
    AVGPartialType<InputType> combine(AVGPartialType<InputType> partialValue, AVGPartialType<InputType> inputValue) override {
        partialValue.count = +inputValue.count;
        partialValue.sum = +inputValue.sum;
        return partialValue;
    }

    /*
     * @brief maps partial aggregates to an element of FinalAggregationType
     * @param partial aggregate element
     * @return element mapped to FinalAggregationType
     */
    AVGResultType lower(AVGPartialType<InputType> partialAggregateValue) override {
        return partialAggregateValue.sum / partialAggregateValue.count;
    }

  protected:
};

}// namespace NES

#endif//NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEAVGAGGREGATION_HPP_
