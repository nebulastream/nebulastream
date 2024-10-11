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

#pragma once
#include <Measures/TimeMeasure.hpp>
#include <Types/TimeBasedWindowType.hpp>
namespace NES::Windowing
{
/**
 * A SlidingWindow assigns records to multiple overlapping windows.
 */
class SlidingWindow : public TimeBasedWindowType
{
public:
    static WindowTypePtr of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide);

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

    uint64_t hash() const override;

private:
    SlidingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide);
    const TimeMeasure size;
    const TimeMeasure slide;
};

}
