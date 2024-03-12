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

#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Storage/AllEntriesMetricStore.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <chrono>

namespace NES::Monitoring {

AllEntriesMetricStore::AllEntriesMetricStore() { NES_DEBUG("AllEntriesMetricStore: Init NewestMetricStore"); }

MetricStoreType AllEntriesMetricStore::getType() const { return MetricStoreType::AllEntries; }

void AllEntriesMetricStore::addMetrics(WorkerId nodeId, MetricPtr metric) {
    std::unique_lock lock(storeMutex);
    StoredNodeMetricsPtr nodeMetrics;
    uint64_t timestamp = duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (storedMetrics.contains(nodeId)) {
        NES_TRACE("AllEntriesMetricStore: Found stored metrics for node with ID {}", nodeId);
        nodeMetrics = storedMetrics[nodeId];
        // check if the metric type exists
        if (!nodeMetrics->contains(metric->getMetricType())) {
            NES_TRACE("AllEntriesMetricStore: Creating metrics {} of {}",
                      nodeId,
                      std::string(magic_enum::enum_name(metric->getMetricType())));
            nodeMetrics->insert({metric->getMetricType(), std::make_shared<std::vector<TimestampMetricPtr>>()});
        }
    } else {
        NES_TRACE("AllEntriesMetricStore: Creating node {} of {}",
                  nodeId,
                  std::string(magic_enum::enum_name(metric->getMetricType())));
        nodeMetrics = std::make_shared<std::unordered_map<MetricType, std::shared_ptr<std::vector<TimestampMetricPtr>>>>();
        nodeMetrics->insert({metric->getMetricType(), std::make_shared<std::vector<TimestampMetricPtr>>()});
        storedMetrics.emplace(nodeId, nodeMetrics);
    }
    NES_TRACE("AllEntriesMetricStore: Adding metrics for {} with type {}: {}",
              nodeId,
              std::string(magic_enum::enum_name(metric->getMetricType())),
              NES::Monitoring::asJson(metric));
    TimestampMetricPtr entry = std::make_shared<std::pair<uint64_t, MetricPtr>>(timestamp, metric);
    auto entryVec = nodeMetrics->at(metric->getMetricType());
    entryVec->emplace_back(std::move(entry));
}

bool AllEntriesMetricStore::removeMetrics(WorkerId nodeId) {
    std::unique_lock lock(storeMutex);
    if (storedMetrics.contains(nodeId)) {
        storedMetrics.erase(nodeId);
        return true;
    }
    return false;
}

bool AllEntriesMetricStore::hasMetrics(WorkerId nodeId) {
    std::unique_lock lock(storeMutex);
    return storedMetrics.contains(nodeId);
}

StoredNodeMetricsPtr AllEntriesMetricStore::getAllMetrics(WorkerId nodeId) {
    std::unique_lock lock(storeMutex);
    return storedMetrics[nodeId];
}

}// namespace NES::Monitoring