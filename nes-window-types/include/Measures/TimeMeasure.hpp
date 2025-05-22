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
#include <cstdint>
#include <Measures/WindowMeasure.hpp>

namespace NES::Windowing
{

/**
 * A time based window measure.
 */
class TimeMeasure : public WindowMeasure
{
public:
    /**
     * @brief Constructor for a TimeMeasure
     * @param milliseconds
     */
    explicit TimeMeasure(uint64_t milliseconds);

    /**
     * @brief gets the time measure in milliseconds
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getTime() const;

    std::string toString() const override;

    /**
     * @brief Compares for equality
     * @param other: TimeMeasure
     * @return Boolean
     */
    bool equals(const TimeMeasure& other) const;

    /**
     * @brief Checks for less than
     * @param other
     * @return True if this < other, false otherwise
     */
    bool operator<(const TimeMeasure& other) const;

    /**
     * @brief Checks for less or equal than
     * @param other
     * @return True if this <= other, false otherwise
     */
    bool operator<=(const TimeMeasure& other) const;

    /**
     * @brief Checks equality
     * @param other
     * @return True if this == other, false otherwise
     */
    bool operator==(const TimeMeasure& other) const;

private:
    const uint64_t milliSeconds;
};

}
