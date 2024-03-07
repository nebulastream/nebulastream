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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICKEY_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICKEY_HPP_

#include <Operators/LogicalOperators/StatisticCollection/Characteristic/Characteristic.hpp>

namespace NES::Statistic {

/*
 * We use this as a way to not having to send the characteristic and window to all workers.
 * The StatisticHash uniquely identifies a statistic anywhere in our system, which means we do not need to send
 * characteristics and windows to all workers to uniquely identify a statistic.
 */
using StatisticHash = uint64_t;
// a fixed-point representation of the fractional part of the golden ratio to provide better distribution properties in hash functions
constexpr auto goldenRatio = 0x9e3779b97f4a7c15;

/**
 * @brief This class represents how to uniquely identify a statistic in the system. It consists of a metric (Type over a field)
 * and a StatisticId. The statistic id is unique across all components, e.g., physical sources and operators.
 * By combining them, we can specify what metric is being tracked for what component. One example might be tracking
 * the selectivity over the physical source `car_1`.
 */
class StatisticKey {
  public:
    /**
     * @brief Constructor for a StatisticKey
     * @param metric
     * @param statisticId represents the unique identifier of components that we can track statistics for
     */
    StatisticKey(MetricPtr metric, StatisticId statisticId);

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if equal otherwise false
     */
    bool operator==(const StatisticKey& rhs) const;

    /**
     * @brief Checks for equality
     * @param rhs
     * @return True, if NOT equal otherwise false
     */
    bool operator!=(const StatisticKey& rhs) const;

    /**
     * @brief Calculates the hash for this statistic key
     * @return std::size_t
     */
    StatisticHash hash() const;

    /**
     * @brief Combines the metricHash and the statisticId to a StatisticHash. We need this as a static hash function,
     * as we can therefore provide the metricHash to the nautilus operator instead of having to send the whole metric
     * down to the nautilus operator.
     * @param metricHash: Hash of the metric, i.e., metric->hash()
     * @param statisticId: StatisticId identifying the component over which we want to collect statistics
     * @return StatisticHash
     */
    static StatisticHash combineStatisticIdWithMetricHash(MetricHash metricHash, StatisticId statisticId);

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    std::string toString() const;

  private:
    const MetricPtr metric;
    const StatisticId statisticId;
};

/**
 * @brief Necessary for unordered_map with StatisticKey as the key
 */
struct StatisticKeyHash {
    StatisticHash operator()(const StatisticKey& key) const { return key.hash(); }
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICKEY_HPP_
