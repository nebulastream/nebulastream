#ifndef NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLECOUNTAGGREGATION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLECOUNTAGGREGATION_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>
#include <utility>
#include <Windowing/WindowAggregations/ExecutableWindowAggregation.hpp>
#include <memory>
namespace NES::Windowing {

typedef uint64_t CountType;

/**
 * @brief A executable window aggregation, which is typed for the correct input, partial, and final data types.
 * @tparam InputType input type of the aggregation
 * @tparam PartialAggregateType partial aggregation type
 * @tparam FinalAggregateType final aggregation type
 */
template<typename InputType>
class ExecutableCountAggregation : public  ExecutableWindowAggregation<InputType, CountType, CountType>{
  public:
    ExecutableCountAggregation(): ExecutableWindowAggregation<InputType, CountType, CountType>(){
    };

    static std::shared_ptr<ExecutableWindowAggregation<InputType, CountType, CountType>> create(){
        return std::make_shared<ExecutableCountAggregation<InputType>>();
    };

    /*
     * @brief maps the input element to an element PartialAggregateType
     * @param input value of the element
     * @return the element that mapped to PartialAggregateType
     */
    CountType lift(InputType) override {
        return 1;
    }

    /*
     * @brief combines two partial aggregates to a new partial aggregate
     * @param current partial value
     * @param the new input element
     * @return new partial aggregate as combination of partialValue and inputValue
     */
    CountType combine(CountType partialValue, CountType inputValue) override {
       return partialValue + inputValue;
    }

    /*
     * @brief maps partial aggregates to an element of FinalAggregationType
     * @param partial aggregate element
     * @return element mapped to FinalAggregationType
     */
    CountType lower(CountType partialAggregateValue) override {
        return partialAggregateValue;
    }
};

}

#endif//NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLECOUNTAGGREGATION_HPP_
