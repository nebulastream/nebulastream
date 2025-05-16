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
#include <vector>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/WindowType.hpp>
namespace NES::Windowing
{

class TimeBasedWindowType : public WindowType
{
public:
    explicit TimeBasedWindowType(TimeCharacteristic timeCharacteristic);

    ~TimeBasedWindowType() override = default;

    [[nodiscard]] TimeCharacteristic getTimeCharacteristic() const;

    /// @brief method to get the window size
    /// @return size of window
    virtual TimeMeasure getSize() = 0;

    /// @brief method to get the window slide
    /// @return slide of the window
    virtual TimeMeasure getSlide() = 0;

    /// @brief Infer dataType of time based window type
    /// @param schema : the schema of the window
    /// @return true if success else false
    bool inferStamp(const Schema& schema) override;

protected:
    TimeCharacteristic timeCharacteristic;
};

}
