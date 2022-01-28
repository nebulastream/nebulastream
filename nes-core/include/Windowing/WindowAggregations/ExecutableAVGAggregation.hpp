/*
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

#ifndef NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEAVGAGGREGATION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEAVGAGGREGATION_HPP_

#include <Windowing/WindowAggregations/ExecutableWindowAggregation.hpp>
#include <memory>
#include <type_traits>
#include <utility>
namespace NES::Windowing {

template<typename SumType, std::enable_if_t<std::is_arithmetic<SumType>::value, bool> = true>
class AVGPartialType {
  public:
    explicit AVGPartialType(SumType sum) : sum(sum), count(1) {}
    explicit AVGPartialType() : sum(0) {}

    [[nodiscard]] SumType getSum() const { return sum; }
    [[nodiscard]] int64_t getCount() const { return count; }

    void addToSum(SumType value) { sum += value; }
    void addToCount(int64_t value = 1) { count += value; }

  private:
    SumType sum;
    int64_t count{0};
};

using AVGResultType = double;

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
    AVGPartialType<InputType> lift(InputType inputValue) override { return AVGPartialType<InputType>(inputValue); }

    /*
     * @brief combines two partial aggregates to a new partial aggregate
     * @param current partial value
     * @param the new input element
     * @return new partial aggregate as combination of partialValue and inputValue
     */
    AVGPartialType<InputType> combine(AVGPartialType<InputType> partialValue, AVGPartialType<InputType> inputValue) override {
        partialValue.addToSum(inputValue.getSum());
        partialValue.addToCount(inputValue.getCount());
        return partialValue;
    }

    /*
     * @brief maps partial aggregates to an element of FinalAggregationType
     * @param partial aggregate element
     * @return element mapped to FinalAggregationType
     */
    AVGResultType lower(AVGPartialType<InputType> partialAggregateValue) override {
        return (AVGResultType) partialAggregateValue.getSum() / (AVGResultType) partialAggregateValue.getCount();
    }
};

}// namespace NES::Windowing

#endif  // NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEAVGAGGREGATION_HPP_
