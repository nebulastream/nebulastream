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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_BUFFERRATE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_BUFFERRATE_HPP_
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/StatisticMetric.hpp>

namespace NES::Statistic {

/**
 * @brief Collects the rate of buffers are used on a node. A positive number means that there are more buffer being
 * freed than allocated/acquired. A negative number the opposite. The unit is buffers/second
 */
class BufferRate : public StatisticMetric {
  public:
    /**
     * @brief Creates a BufferRate wrapped in a MetricPtr
     * @return MetricPtr
     */
    static MetricPtr create();

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    bool operator==(const StatisticMetric& rhs) const override;

    /**
     * @brief Creates a string representation
     * @return String representation
     */
    std::string toString() const override;

  private:
    /**
     * @brief Private constructor for BufferRate
     */
    explicit BufferRate();
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTIC_BUFFERRATE_HPP_
