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

#ifndef NES_EXECUTABLEMEDIANAGGREGATION_HPP
#define NES_EXECUTABLEMEDIANAGGREGATION_HPP

#include <Windowing/WindowAggregations/ExecutableWindowAggregation.hpp>

namespace NES::Windowing {

using MedianResultType = double; //use double as the result can be the average of two middle values
/**
 * @brief A executable window aggregation, which is typed for the correct input, partial, and final data types.
 * @tparam InputType input type of the aggregation
 * @note using std::vector<InputType> as partial aggregate type
 */
template<typename InputType, std::enable_if_t<std::is_arithmetic<InputType>::value, int> = 0>
class ExecutableMedianAggregation : public ExecutableWindowAggregation<InputType, std::vector<InputType>, MedianResultType> {
  public:
    ExecutableMedianAggregation() : ExecutableWindowAggregation<InputType, std::vector<InputType>, MedianResultType>(){};

    static std::shared_ptr<ExecutableWindowAggregation<InputType, std::vector<InputType>, MedianResultType>> create() {
        return std::make_shared<ExecutableMedianAggregation<InputType>>();
    };

    /*
     * @brief maps the input element to an element PartialAggregateType
     * @param input value of the element
     * @return the element that mapped to PartialAggregateType
     */
    std::vector<InputType> lift(InputType inputValue) override {
        return {inputValue};
    }

    /*
     * @brief combines two partial aggregates to a new partial aggregate
     * @param current partial value
     * @param the new input element
     * @return new partial aggregate as combination of partialValue and inputValue
     */
    std::vector<InputType> combine(std::vector<InputType> partialValue, std::vector<InputType> inputValue) override {
        partialValue.insert(partialValue.end(), inputValue.begin(), inputValue.end());
        return partialValue;
    }

    /*
     * @brief maps partial aggregates to an element of FinalAggregationType
     * @param partial aggregate element
     * @return element mapped to FinalAggregationType
     */
    MedianResultType lower(std::vector<InputType> partialAggregateValue) override {
        // sort the partial aggregate vector before finding the median
        std::sort(partialAggregateValue.begin(), partialAggregateValue.end());

        // compute median depending whether the size of partial aggregate vector is even or odd
        MedianResultType median;
        if (partialAggregateValue.size() % 2 == 0) {
            // return the average of two middle value
            auto idx1 = (partialAggregateValue.size() / 2) - 1;
            auto idx2 = partialAggregateValue.size() / 2;

            median = (partialAggregateValue[idx1] + partialAggregateValue[idx2]) / 2.0;
        } else {
            // return the middle value
            auto idx = (partialAggregateValue.size() / 2);
            median = partialAggregateValue[idx];
        }

        return median;
    }
};
}// namespace NES::Windowing

#endif//NES_EXECUTABLEMEDIANAGGREGATION_HPP
