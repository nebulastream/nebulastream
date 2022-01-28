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

#ifndef NES_INCLUDE_MONITORING_METRICSTORE_HPP_
#define NES_INCLUDE_MONITORING_METRICSTORE_HPP_

#include <Util/Logger.hpp>
#include <cstdint>
#include <queue>
#include <unordered_map>

namespace NES {

class GroupedMetricValues;
using GroupedMetricValuesPtr = std::shared_ptr<GroupedMetricValues>;

enum MetricStoreStrategy { NEWEST, ALWAYS };

/**
 * @brief The MetricStore that stores all the metrics for monitoring.
 */
class MetricStore {
  public:
    explicit MetricStore(MetricStoreStrategy storeType);

    /**
     * @brief Add a metric for a given node by ID
     * @param nodeId
     * @param metrics
     */
    void addMetric(uint64_t nodeId, GroupedMetricValuesPtr metrics);

    /**
     * @brief Get metrics for a given node by ID ordered by the timestamp of the grouped metrics.
     * @param nodeId
     * @return The grouped metrics
     */
    std::priority_queue<GroupedMetricValuesPtr> getMetrics(uint64_t nodeId);

    /**
     * @brief Get most recent metric in store
     * @param nodeId
     * @return the metric
     */
    GroupedMetricValuesPtr getNewestMetric(uint64_t nodeId);

    /**
     * @brief remove a given metric
     * @param true if metric existed and was removed, else false
     */
    bool removeMetrics(uint64_t nodeId);

    /**
     * Checks if metrics exist for a given node
     * @param nodeId
     * @return True if exists, else false
     */
    bool hasMetric(uint64_t nodeId);

  private:
    MetricStoreStrategy storeType;
    std::unordered_map<uint64_t, std::priority_queue<GroupedMetricValuesPtr>> storedMetrics;
};

using MetricStorePtr = std::shared_ptr<MetricStore>;

}// namespace NES

#endif  // NES_INCLUDE_MONITORING_METRICSTORE_HPP_
