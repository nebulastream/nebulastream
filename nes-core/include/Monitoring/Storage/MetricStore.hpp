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

#ifndef NES_INCLUDE_MONITORING_METRIC_STORE_HPP_
#define NES_INCLUDE_MONITORING_METRIC_STORE_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <cstdint>
#include <unordered_map>

namespace NES {

enum MetricStoreStrategy { NEWEST };
enum MetricCollectorType {};

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
    void addMetrics(uint64_t nodeId, MetricPtr metrics);

    /**
     * @brief Get most recent metric in store
     * @param nodeId
     * @return the metric
    */
    std::unordered_map<MetricType, MetricPtr> getNewestMetrics(uint64_t nodeId);

    /**
     * @brief Remove all metrics for a given node.
     * @param true if metric existed and was removed, else false
    */
    bool removeMetrics(uint64_t nodeId);

    /**
     * Checks if any kind of metrics are stored for a given node
     * @param nodeId
     * @return True if exists, else false
    */
    bool hasMetrics(uint64_t nodeId);

  private:
    MetricStoreStrategy storeType;
    std::unordered_map<uint64_t, std::unordered_map<MetricType, MetricPtr>> storedMetrics;
};

using MetricStorePtr = std::shared_ptr<MetricStore>;

}// namespace NES

#endif// NES_INCLUDE_MONITORING_METRIC_STORE_HPP_
