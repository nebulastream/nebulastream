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

#include <memory>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES::Windowing
{
/**
 * A TumblingWindow assigns records to non-overlapping windows.
 */
class TumblingWindow : public TimeBasedWindowType
{
public:
    /**
    * Creates a new TumblingWindow that assigns
    * elements to time windows based on the element timestamp and multiplier.
    * For example, if you want window a stream by hour,but window begins at the 15th minutes
    * of each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get
    * time windows start at 0:15:00,1:15:00,2:15:00,etc.
    * @param size
    * @return std::shared_ptr<WindowType>
    */
    static std::shared_ptr<WindowType> of(std::shared_ptr<TimeCharacteristic> timeCharacteristic, TimeMeasure size);

    /**
    * @brief return size of the window
    * @return size of the window
    */
    TimeMeasure getSize() override;

    TimeMeasure getSlide() override;

    std::string toString() const override;

    bool equal(std::shared_ptr<WindowType> otherWindowType) override;

private:
    TumblingWindow(std::shared_ptr<TimeCharacteristic> timeCharacteristic, TimeMeasure size);
    const TimeMeasure size;
};

}
