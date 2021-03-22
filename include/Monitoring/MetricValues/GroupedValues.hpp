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

#ifndef NES_INCLUDE_MONITORING_METRICVALUES_GROUPEDVALUES_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_GROUPEDVALUES_HPP_

#include <Monitoring/MetricValues/CpuMetrics.hpp>
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/MetricValues/MemoryMetrics.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>

namespace NES {
class MonitoringPlan;

struct GroupedValues {
    std::optional<std::unique_ptr<DiskMetrics>> diskMetrics;
    std::optional<std::unique_ptr<CpuMetrics>> cpuMetrics;
    std::optional<std::unique_ptr<NetworkMetrics>> networkMetrics;
    std::optional<std::unique_ptr<MemoryMetrics>> memoryMetrics;

    web::json::value asJson() {
        web::json::value metricsJson{};

        if (diskMetrics.has_value()) {
            metricsJson["disk"] =  diskMetrics.value()->toJson();
        }

        if (cpuMetrics.has_value()) {
            metricsJson["cpu"] =  cpuMetrics.value()->toJson();
        }

        if (networkMetrics.has_value()) {
            metricsJson["network"] =  networkMetrics.value()->toJson();
        }

        if (memoryMetrics.has_value()) {
            metricsJson["memory"] =  memoryMetrics.value()->toJson();
        }

        return metricsJson;
    };
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_GROUPEDVALUES_HPP_
