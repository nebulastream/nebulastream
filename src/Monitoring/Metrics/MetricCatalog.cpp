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

#include <Monitoring/Metrics/MetricCatalog.hpp>

#include <Monitoring/Metrics/Metric.hpp>
#include <utility>

namespace NES {

MetricCatalog::MetricCatalog(std::map<MetricValueType, Metric> metrics) : metricValueTypeToMetricMap(std::move(metrics)) {
}

std::shared_ptr<MetricCatalog> MetricCatalog::create(std::map<MetricValueType, Metric> metrics) {
    return std::make_shared<MetricCatalog>(MetricCatalog(std::move(metrics)));
}

std::shared_ptr<MetricCatalog> MetricCatalog::NesMetrics() {
    auto nesMetrics = std::map<MetricValueType, Metric>();
    return create(nesMetrics);
}

bool MetricCatalog::add(MetricValueType type, const Metric&& metric) {
    if (metricValueTypeToMetricMap.count(type) > 0) {
        return false;
    } else {
        metricValueTypeToMetricMap.insert(std::pair<MetricValueType, Metric>(type, metric));
        return false;
    }
}

}// namespace NES