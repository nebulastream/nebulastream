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

#ifndef NES_NES_RUNTIME_INCLUDE_STATISTICS_INTERVAL_HPP_
#define NES_NES_RUNTIME_INCLUDE_STATISTICS_INTERVAL_HPP_

#include <cstddef>
#include <functional>
#include <cstdint>

namespace NES::Experimental::Statistics {

/**
 * @brief This class simply defines a (time) interval
 */
class Interval {
  public:
    /**
     * @param startTime the start of the interval
     * @param endTime  the end of the interval
     */
    explicit Interval(const uint64_t startTime, const uint64_t endTime);

    /**
     * @brief the default destructor of an Interval
     */
    virtual ~Interval() = default;

    /**
     * @brief we implement a equal operator for the interval, as well as a hash function, such that it can be used as a key in a hash-map
     * @param interval a interval with a start and a stopTime
     * @return true if two intervals are equal
     */
    bool operator==(const Interval& interval) const;

    /**
     * @brief a hash function to use the interval as a key
     */
    struct Hash {
        std::size_t operator()(const Interval& interval) const {
            std::size_t hashValue = 0;
            hashValue ^= std::hash<uint64_t>()(interval.startTime);
            hashValue ^= std::hash<uint64_t>()(interval.endTime);
            return hashValue;
        }
    };

    /**
     * @return returns the startTime of the Interval
     */
    uint64_t getStartTime() const;

    /**
     * @return returns the endTime of the Interval
     */
    uint64_t getEndTime() const;

  private:
    uint64_t startTime;
    uint64_t endTime;
};
}
#endif//NES_NES_RUNTIME_INCLUDE_STATISTICS_INTERVAL_HPP_
