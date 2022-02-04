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
#include <Monitoring/Storage/MetricStore.hpp>
#include <Util/Logger.hpp>
#include <Monitoring/Metrics/Metric.hpp>

namespace NES {

    MetricStore::MetricStore(MetricStoreStrategy storeType) : storeType(storeType) {
        NES_DEBUG("MetricStore: Init with store type " << storeType);
    }

    void MetricStore::addMetric(uint64_t nodeId, std::vector<MetricPtr> metrics) {
        std::unordered_map<MetricType, MetricPtr> metricEntry;
        for (auto metric: metrics) {
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

    std::priority_queue<GroupedMetricValuesPtr> MetricStore::getMetrics(uint64_t nodeId) { return storedMetrics[nodeId]; }

    GroupedMetricValuesPtr MetricStore::getNewestMetric(uint64_t nodeId) { return storedMetrics[nodeId].top(); }

    bool MetricStore::removeMetrics(uint64_t nodeId) {
        if (storedMetrics.contains(nodeId)) {
            storedMetrics.erase(nodeId);
            return true;
        }
        return false;
    }

    bool MetricStore::hasMetric(uint64_t nodeId) {
        if (storedMetrics.contains(nodeId)) {
            return true;
        }
        return false;
    }

}// namespace NES