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

#ifndef NES_CORE_INCLUDE_MONITORING_STORAGE_ABSTRACTMETRICSTORE_HPP_
#define NES_CORE_INCLUDE_MONITORING_STORAGE_ABSTRACTMETRICSTORE_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Monitoring/Storage/MetricStoreType.hpp>
#include <cstdint>
#include <unordered_map>
#include <vector>

namespace NES::Monitoring {
/**
* @brief The LatestEntriesMetricStore that stores all the metrics for monitoring.
*/
class AbstractMetricStore {
  public:
    //  -- dtor --
    virtual ~AbstractMetricStore() = default;

    /**
     * @brief Returns the type of storage.
     * @return The storage type.
     */
    virtual MetricStoreType getType() const = 0;

    /**
     * @brief Add a metric for a given node by ID
     * @param nodeId
     * @param metrics
    */
    virtual void addMetrics(uint64_t nodeId, MetricPtr metrics) = 0;

    /**
     * @brief Get newest metrics from store
     * @param nodeId
     * @return the metric
    */
    virtual StoredNodeMetricsPtr getAllMetrics(uint64_t nodeId) = 0;

    /**
     * @brief Remove all metrics for a given node.
     * @param true if metric existed and was removed, else false
    */
    virtual bool removeMetrics(uint64_t nodeId) = 0;

    /**
     * Checks if any kind of metrics are stored for a given node
     * @param nodeId
     * @return True if exists, else false
    */
    virtual bool hasMetrics(uint64_t nodeId) = 0;
};
}// namespace NES::Monitoring

#endif// NES_CORE_INCLUDE_MONITORING_STORAGE_ABSTRACTMETRICSTORE_HPP_
