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
#include <chrono>

namespace NES {

AllEntriesMetricStore::AllEntriesMetricStore() { NES_DEBUG("AllEntriesMetricStore: Init NewestMetricStore"); }

MetricStoreType AllEntriesMetricStore::getType() const { return AllEntries; }

void AllEntriesMetricStore::addMetrics(uint64_t nodeId, MetricPtr metric) {
    NES_DEBUG("AllEntriesMetricStore: Adding metric of type " << toString(metric->getMetricType()));
    uint64_t timestamp = duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (storedMetrics.contains(nodeId)) {
        NES_DEBUG("AllEntriesMetricStore: Found stored metrics for node with ID " << nodeId);
        StoredNodeMetricsPtr nodeMetrics = storedMetrics[nodeId];
        // check if the metric type exists
        if (nodeMetrics->contains(metric->getMetricType())) {
            NES_DEBUG("AllEntriesMetricStore: Found metrics for " << nodeId << " of type " << toString(metric->getMetricType()));
            MetricEntryPtr entry = std::make_shared<std::pair<uint64_t, MetricPtr>>(timestamp, metric);
            auto entryVec = nodeMetrics->at(metric->getMetricType());
            entryVec->emplace_back(std::move(entry));
        } else {
            NES_DEBUG("AllEntriesMetricStore: Creating metrics " << nodeId << " of type " << toString(metric->getMetricType()));
            MetricEntryPtr metricEntry = std::make_shared<std::pair<uint64_t, MetricPtr>>(timestamp, metric);
            auto entryVec = std::make_shared<std::vector<MetricEntryPtr>>();
            entryVec->emplace_back(std::move(metricEntry));
            nodeMetrics->insert({metric->getMetricType(), std::move(entryVec)});
        }
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

bool AllEntriesMetricStore::removeMetrics(uint64_t nodeId) {
    if (storedMetrics.contains(nodeId)) {
        storedMetrics.erase(nodeId);
        return true;
    }
    return false;
}

bool AllEntriesMetricStore::hasMetrics(uint64_t nodeId) { return storedMetrics.contains(nodeId); }

StoredNodeMetricsPtr AllEntriesMetricStore::getAllMetrics(uint64_t nodeId) { return storedMetrics[nodeId]; }

}// namespace NES