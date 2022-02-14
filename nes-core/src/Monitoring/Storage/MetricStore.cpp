/*
Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

void MetricStore::addMetrics(uint64_t nodeId, const std::vector<MetricPtr>& metrics) {
    std::unordered_map<MetricType, MetricPtr> metricEntry;
    for (const auto& metric : metrics) {
        metricEntry.insert({getMetricType(metric), metric});
    }

    if (storeType == MetricStoreStrategy::NEWEST) {
        if (storedMetrics.contains(nodeId)) {
            storedMetrics[nodeId].swap(metricEntry);
        } else {
            storedMetrics.emplace(nodeId, std::move(metricEntry));
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

std::vector<MetricPtr> MetricStore::getNewestMetrics(uint64_t nodeId) {
    using MetricMap = std::unordered_map<MetricType, MetricPtr>;
    auto metricMap = storedMetrics.at(nodeId);
    std::vector<MetricPtr> output;
    transform(metricMap.begin(), metricMap.end(), back_inserter(output), [](const MetricMap::value_type& val) {
        return val.second;
    });
    return output;
}

}// namespace NES