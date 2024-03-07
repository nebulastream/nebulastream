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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_TYPES_SLIDINGWINDOW_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_TYPES_SLIDINGWINDOW_HPP_
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
namespace NES::Windowing {
/**
 * A SlidingWindow assigns records to multiple overlapping windows.
 */
class SlidingWindow : public TimeBasedWindowType {
  public:
    static WindowTypePtr
    of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide, std::optional<TimeCharacteristicPtr> other = std::nullopt);

    /**
    * @brief return size of the window
    * @return size of the window
    */
    TimeMeasure getSize() override;

    /**
    * @brief return size of the slide
    * @return size of the slide
    */
    TimeMeasure getSlide() override;

    std::string toString() const override;

    bool equal(WindowTypePtr otherWindowType) override;

  private:
    SlidingWindow(TimeCharacteristicPtr timeCharacteristic,
                  TimeMeasure size,
                  TimeMeasure slide,
                  std::optional<TimeCharacteristicPtr> other);
    const TimeMeasure size;
    const TimeMeasure slide;
};

}// namespace NES::Windowing

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_TYPES_SLIDINGWINDOW_HPP_
