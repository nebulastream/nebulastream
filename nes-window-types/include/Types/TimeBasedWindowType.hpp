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
#include <API/Schema.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Types/WindowType.hpp>

namespace NES::Windowing
{


class TimeBasedWindowType : public WindowType
{
public:
    explicit TimeBasedWindowType(std::shared_ptr<TimeCharacteristic> timeCharacteristic);

    ~TimeBasedWindowType() override = default;

    [[nodiscard]] std::shared_ptr<TimeCharacteristic> getTimeCharacteristic() const;

    /**
     * @brief method to get the window size
     * @return size of window
     */
    virtual TimeMeasure getSize() = 0;

    /**
     * @brief method to get the window slide
     * @return slide of the window
     */
    virtual TimeMeasure getSlide() = 0;

    bool inferStamp(const Schema& schema) override;

protected:
    std::shared_ptr<TimeCharacteristic> timeCharacteristic;
};

}
