#ifndef NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLESUMAGGREGATION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLESUMAGGREGATION_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>
#include <utility>
#include <Windowing/WindowAggregations/ExecutableWindowAggregation.hpp>
#include <memory>
#include <type_traits>
namespace NES::Windowing {

/**
 * @brief A executable window aggregation, which is typed for the correct input, partial, and final data types.
 * @tparam InputType input type of the aggregation
 * @tparam PartialAggregateType partial aggregation type
 * @tparam FinalAggregateType final aggregation type
 */
template<typename InputType, std::enable_if_t<std::is_arithmetic<InputType>::value, int> = 0>
class ExecutableSumAggregation : public  ExecutableWindowAggregation<InputType, InputType, InputType>{
  public:
    ExecutableSumAggregation(): ExecutableWindowAggregation<InputType, InputType, InputType>(){
    };

    static std::shared_ptr<ExecutableWindowAggregation<InputType, InputType, InputType>> create(){
        return std::make_shared<ExecutableSumAggregation<InputType>>();
    };

    /*
     * @brief maps the input element to an element PartialAggregateType
     * @param input value of the element
     * @return the element that mapped to PartialAggregateType
     */
    InputType lift(InputType inputValue) override {
        return inputValue;
    }

    /*
     * @brief combines two partial aggregates to a new partial aggregate
     * @param current partial value
     * @param the new input element
     * @return new partial aggregate as combination of partialValue and inputValue
     */
    InputType combine(InputType partialValue, InputType inputValue) override {
       return partialValue + inputValue;
    }

    /*
     * @brief maps partial aggregates to an element of FinalAggregationType
     * @param partial aggregate element
     * @return element mapped to FinalAggregationType
     */
    InputType lower(InputType partialAggregateValue) override {
        return partialAggregateValue;
    }
};

}

#endif//NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLESUMAGGREGATION_HPP_
