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

/// A SlidingWindow assigns records to multiple overlapping windows.
class SlidingWindow : public TimeBasedWindowType
{
public:
    static std::shared_ptr<WindowType> of(TimeCharacteristic timeCharacteristic, TimeMeasure size, TimeMeasure slide);

    TimeMeasure getSize() override;
    TimeMeasure getSlide() override;

    std::string toString() const override;

    bool operator==(const WindowType& otherWindowType) const override;

private:
    SlidingWindow(TimeCharacteristic timeCharacteristic, TimeMeasure size, TimeMeasure slide);
    const TimeMeasure size;
    const TimeMeasure slide;
};

}
