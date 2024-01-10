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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_STORAGE_ALLENTRIESMETRICSTORE_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_STORAGE_ALLENTRIESMETRICSTORE_HPP_

#include <Monitoring/Storage/AbstractMetricStore.hpp>
#include <mutex>

namespace NES::Monitoring {

/**
* @brief The AllEntriesMetricStore that stores all the metrics for monitoring.
*/
class AllEntriesMetricStore : public AbstractMetricStore {
  public:
    explicit AllEntriesMetricStore();
    ~AllEntriesMetricStore() = default;

    /**
     * @brief Returns the type of storage.
     * @return The storage type.
     */
    virtual MetricStoreType getType() const override;

    /**
     * @brief Add a metric for a given node by ID
     * @param nodeId
     * @param metrics
    */
    void addMetrics(uint64_t nodeId, MetricPtr metrics) override;

    /**
     * @brief Get latest metrics from store
     * @param nodeId
     * @return the metric
    */
    virtual StoredNodeMetricsPtr getAllMetrics(uint64_t nodeId) override;

    /**
     * @brief Remove all metrics for a given node.
     * @param true if metric existed and was removed, else false
    */
    bool removeMetrics(uint64_t nodeId) override;

    /**
     * Checks if any kind of metrics are stored for a given node
     * @param nodeId
     * @return True if exists, else false
    */
    bool hasMetrics(uint64_t nodeId) override;

  private:
    std::unordered_map<uint64_t, StoredNodeMetricsPtr> storedMetrics;
    mutable std::mutex storeMutex;
};

}// namespace NES::Monitoring

#endif // NES_RUNTIME_INCLUDE_MONITORING_STORAGE_ALLENTRIESMETRICSTORE_HPP_
