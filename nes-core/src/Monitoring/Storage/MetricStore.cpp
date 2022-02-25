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
#include <Monitoring/Storage/MetricStore.hpp>
#include <Util/Logger.hpp>

namespace NES {

MetricStore::MetricStore(MetricStoreStrategy storeType) : storeType(storeType) {
    NES_DEBUG("MetricStore: Init with store type " << storeType);
}

void MetricStore::addMetrics(uint64_t nodeId, MetricPtr metric) {
    NES_DEBUG("MetricStore: Adding metric of type " << toString(metric->getMetricType()));
    if (storeType == MetricStoreStrategy::NEWEST) {
        if (storedMetrics.contains(nodeId)) {
            std::unordered_map<MetricType, MetricPtr>& entry = storedMetrics[nodeId];
            entry[metric->getMetricType()] = metric;
        } else {
            std::unordered_map<MetricType, MetricPtr> metricEntry;
            metricEntry.insert({metric->getMetricType(), metric});
            storedMetrics.emplace(nodeId, metricEntry);
        }
    } else {
        NES_THROW_RUNTIME_ERROR("MetricStore: StoreType " << storeType << " not found.");
    }
}

bool MetricStore::removeMetrics(uint64_t nodeId) {
    if (storedMetrics.contains(nodeId)) {
        storedMetrics.erase(nodeId);
        return true;
    }
    return false;
}

bool MetricStore::hasMetrics(uint64_t nodeId) { return storedMetrics.contains(nodeId); }

std::unordered_map<MetricType, MetricPtr> MetricStore::getNewestMetrics(uint64_t nodeId) { return storedMetrics[nodeId]; }

}// namespace NES