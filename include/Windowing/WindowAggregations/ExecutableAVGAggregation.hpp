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
#include <type_traits>
#include <utility>
namespace NES::Windowing {

template<typename SumType>
class AVGPartialType {
  public:
    explicit AVGPartialType(SumType sum) : sum(sum), count(1) {}
    explicit AVGPartialType() : sum(0), count(1) {}
    SumType sum;
    int64_t count;
};

typedef double AVGResultType;

/**
 * @brief A executable window aggregation, which is typed for the correct input, partial, and final data types.
 * @tparam InputType input type of the aggregation
 */
template<typename InputType, std::enable_if_t<std::is_arithmetic<InputType>::value, int> = 0>
class ExecutableAVGAggregation : public ExecutableWindowAggregation<InputType, AVGPartialType<InputType>, AVGResultType> {
  public:
    ExecutableAVGAggregation() : ExecutableWindowAggregation<InputType, AVGPartialType<InputType>, AVGResultType>(){};

    static std::shared_ptr<ExecutableWindowAggregation<InputType, AVGPartialType<InputType>, AVGResultType>> create() {
        return std::make_shared<ExecutableAVGAggregation<InputType>>();
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
        partialValue.count += inputValue.count;
        partialValue.sum += inputValue.sum;
        return partialValue;
    }

    /*
     * @brief maps partial aggregates to an element of FinalAggregationType
     * @param partial aggregate element
     * @return element mapped to FinalAggregationType
     */
    AVGResultType lower(AVGPartialType<InputType> partialAggregateValue) override {
        return (AVGResultType) partialAggregateValue.sum / (AVGResultType) partialAggregateValue.count;
    }
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEAVGAGGREGATION_HPP_
