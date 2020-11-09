/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEMAXAGGREGATION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEMAXAGGREGATION_HPP_
#include <Windowing/WindowAggregations/ExecutableWindowAggregation.hpp>
#include <memory>
#include <type_traits>
#include <utility>
namespace NES::Windowing {

/**
 * @brief A executable window aggregation, which is typed for the correct input, partial, and final data types.
 * @tparam InputType input type of the aggregation
 */
template<typename InputType, std::enable_if_t<std::is_arithmetic<InputType>::value, int> = 0>
class ExecutableMaxAggregation : public ExecutableWindowAggregation<InputType, InputType, InputType> {
  public:
    ExecutableMaxAggregation() : ExecutableWindowAggregation<InputType, InputType, InputType>(){};

    static std::shared_ptr<ExecutableWindowAggregation<InputType, InputType, InputType>> create() {
        return std::make_shared<ExecutableMaxAggregation<InputType>>();
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
        if (inputValue > partialValue) {
            partialValue = inputValue;
        }
        return partialValue;
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

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEMAXAGGREGATION_HPP_
