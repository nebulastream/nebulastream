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
#include <string>

namespace NES::Windowing
{

/**
 * A time based window measure.
 */
class TimeUnit
{
public:
    explicit TimeUnit(uint64_t offset);

    /**
     * @brief gets the multiplier to convert this to milliseconds
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getMillisecondsConversionMultiplier() const;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    [[nodiscard]] std::string toString() const;

    /**
     * @brief Compares for equality
     * @param other
     * @return Boolean
     */
    bool equals(const TimeUnit& other) const;

    /**
     * @brief A time unit in Milliseconds.
     * @return TimeUnit
     */
    static TimeUnit Milliseconds();

    /**
     * @brief A time unit in Seconds.
     * @return TimeUnit
     */
    static TimeUnit Seconds();

    /**
     * @brief A time unit in Minutes.
     * @return TimeUnit
     */
    static TimeUnit Minutes();

    /**
     * @brief A time unit in Hours.
     * @return TimeUnit
     */
    static TimeUnit Hours();

    /**
     * @brief A time unit in Days.
     * @return TimeUnit
     */
    static TimeUnit Days();

private:
    uint64_t multiplier;
};

}
