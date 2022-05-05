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
#include <Monitoring/Storage/LatestEntriesMetricStore.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/time.h>

namespace NES {

LatestEntriesMetricStore::LatestEntriesMetricStore() { NES_DEBUG("LatestEntriesMetricStore: Init NewestMetricStore"); }

MetricStoreType LatestEntriesMetricStore::getType() const { return NewestEntry; }

void LatestEntriesMetricStore::addMetrics(uint64_t nodeId, MetricPtr metric) {
    std::unique_lock lock(storeMutex);
    NES_DEBUG("LatestEntriesMetricStore: Adding metric of type " << toString(metric->getMetricType()));
    uint64_t timestamp = time(NULL);
    if (storedMetrics.contains(nodeId)) {
        StoredNodeMetricsPtr nodeMetrics = storedMetrics[nodeId];
        // check if the metric type exists
        if (nodeMetrics->contains(metric->getMetricType())) {
            nodeMetrics->erase(metric->getMetricType());
        }
        MetricEntryPtr metricEntry = std::make_shared<std::pair<uint64_t, MetricPtr>>(timestamp, metric);
        auto entryVec = std::make_shared<std::vector<MetricEntryPtr>>();
        entryVec->emplace_back(std::move(metricEntry));
        nodeMetrics->insert({metric->getMetricType(), std::move(entryVec)});
    } else {
        MetricEntryPtr metricEntry = std::make_shared<std::pair<uint64_t, MetricPtr>>(timestamp, metric);
        StoredNodeMetricsPtr nodeMetrics =
            std::make_shared<std::unordered_map<MetricType, std::shared_ptr<std::vector<MetricEntryPtr>>>>();
        auto entryVec = std::make_shared<std::vector<MetricEntryPtr>>();
        entryVec->emplace_back(std::move(metricEntry));
        nodeMetrics->insert({metric->getMetricType(), std::move(entryVec)});
        storedMetrics.emplace(nodeId, nodeMetrics);
    }
}

bool LatestEntriesMetricStore::removeMetrics(uint64_t nodeId) {
    std::unique_lock lock(storeMutex);
    if (storedMetrics.contains(nodeId)) {
        storedMetrics.erase(nodeId);
        return true;
    }
    return false;
}

bool LatestEntriesMetricStore::hasMetrics(uint64_t nodeId) {
    std::unique_lock lock(storeMutex);
    return storedMetrics.contains(nodeId);
}

StoredNodeMetricsPtr LatestEntriesMetricStore::getAllMetrics(uint64_t nodeId) {
    std::unique_lock lock(storeMutex);
    return storedMetrics[nodeId];
}

}// namespace NES