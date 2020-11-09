/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_MONITORING_METRICS_INTCOUNTER_HPP_
#define NES_INCLUDE_MONITORING_METRICS_INTCOUNTER_HPP_

#include <Monitoring/Metrics/Counter.hpp>
#include <cstdint>

namespace NES {

/**
 * @brief A monitoring metric class that represents counters.
 */
class IntCounter : public Counter {
  public:
    explicit IntCounter(uint64_t initCount = 0);

    /**
	 * Increment the current count by 1.
	 */
    void inc() override;

    /**
     * Increment the current count by the given value.
     *
     * @param n value to increment the current count by
     * @return the new value
     */
    uint64_t inc(uint64_t val);

    /**
     * Decrement the current count by 1.
     */
    void dec() override;

    /**
     * Decrement the current count by the given value.
     *
     * @param n value to decrement the current count by
     */
    uint64_t dec(uint64_t val);

    /**
     * Returns the current count.
     *
     * @return current count
     */
    uint64_t getCount() const;

  private:
    uint64_t count;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_INTCOUNTER_HPP_
