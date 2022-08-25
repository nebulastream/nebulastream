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

#include "Monitoring/Metrics/Gauge/DiskMetrics.hpp"
#include "Monitoring/Metrics/MetricType.hpp"
#include "Monitoring/Metrics/Gauge/CpuMetrics.hpp"
#include "Monitoring/Metrics/Gauge/MemoryMetrics.hpp"
#include "Monitoring/Metrics/Gauge/NetworkMetrics.hpp"
#include "Monitoring/Metrics/Gauge/RegistrationMetrics.hpp"
#include "Monitoring/Metrics/Gauge/RuntimeMetrics.hpp"
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <list>

namespace NES {
MonitoringPlan::MonitoringPlan(const std::set<MetricType>& metrics) : metricTypes(metrics) {
    NES_DEBUG("MonitoringPlan: Init with metrics of size " << metrics.size());
}

MonitoringPlan::MonitoringPlan(const std::map<MetricType, std::pair<SchemaPtr, uint64_t>>& metrics) : monitoringPlan(metrics) {
 // TODO: SampleRate hinzufügen
    NES_DEBUG("MonitoringPlan: Init with metrics of size " << metrics.size());
}

MonitoringPlanPtr MonitoringPlan::create(const std::set<MetricType>& metrics) {
    return std::shared_ptr<MonitoringPlan>(new MonitoringPlan(metrics));
}

MonitoringPlanPtr MonitoringPlan::create(const std::map <MetricType, std::pair<SchemaPtr, uint64_t>>& monitoringPlan) {
    return std::shared_ptr<MonitoringPlan>(new MonitoringPlan(monitoringPlan));
}

MonitoringPlanPtr MonitoringPlan::setSchemaJson(web::json::value& configuredMetrics) {
    std::map <MetricType, std::pair<SchemaPtr, uint64_t>> configuredMonitoringPlan;
    SchemaPtr schema;
    web::json::value attributesArray;
    std::list<std::string> attributesList;
    uint64_t sampleRate = 0;
    std::pair<MetricType, std::pair<SchemaPtr, uint64_t>> tempPair;
    if (configuredMetrics["cpu"].is_object()) {
        if (configuredMetrics["cpu"]["sampleRate"].is_number()) {
            sampleRate = configuredMetrics["cpu"]["sampleRate"].as_integer();
        }
        attributesList = MetricUtils::jsonArrayToList(configuredMetrics["cpu"]["attributes"]);
        tempPair = std::make_pair(WrappedCpuMetrics, std::make_pair(CpuMetrics::createSchema("", attributesList), sampleRate));
        configuredMonitoringPlan.insert(tempPair);
    } if (configuredMetrics["disk"].is_object()) {
        if (configuredMetrics["disk"]["sampleRate"].is_number()) {
            sampleRate = configuredMetrics["disk"]["sampleRate"].as_integer();
        } else {
            sampleRate = 0;
        }
        attributesList = MetricUtils::jsonArrayToList(configuredMetrics["disk"]["attributes"]);
        tempPair = std::make_pair(DiskMetric, std::make_pair(DiskMetrics::createSchema("", attributesList), sampleRate));
        configuredMonitoringPlan.insert(tempPair);
    } if (configuredMetrics["memory"].is_object()) {
        if (configuredMetrics["memory"]["sampleRate"].is_number()) {
            sampleRate = configuredMetrics["memory"]["sampleRate"].as_integer();
        } else {
            sampleRate = 0;
        }
        attributesList = MetricUtils::jsonArrayToList(configuredMetrics["memory"]["attributes"]);
        tempPair = std::make_pair(MemoryMetric, std::make_pair(MemoryMetrics::createSchema("", attributesList), sampleRate));
        configuredMonitoringPlan.insert(tempPair);
    } if (configuredMetrics["network"].is_object()) {
        if (configuredMetrics["network"]["sampleRate"].is_number()) {
            sampleRate = configuredMetrics["network"]["sampleRate"].as_integer();

        } else {
            sampleRate = 0;
        }
        attributesList = MetricUtils::jsonArrayToList(configuredMetrics["network"]["attributes"]);
        tempPair = std::make_pair(WrappedNetworkMetrics, std::make_pair(NetworkMetrics::createSchema("", attributesList), sampleRate));
        configuredMonitoringPlan.insert(tempPair);
    }

    return MonitoringPlan::create(configuredMonitoringPlan);
}

SchemaPtr MonitoringPlan::getSchema(MetricType metric) {
    // TODO: auffangen, falls MetricType nicht in MonitoringPlan vorhanden
    return monitoringPlan.find(metric)->second.first;
}

uint64_t MonitoringPlan::getSampleRate(MetricType metric) {
    return monitoringPlan.find(metric)->second.second;
}

MonitoringPlanPtr MonitoringPlan::defaultPlan() {
    std::set<MetricType> metricTypes{WrappedCpuMetrics, DiskMetric, MemoryMetric, WrappedNetworkMetrics};
    return MonitoringPlan::create(metricTypes);
}

std::set<MetricCollectorType> MonitoringPlan::defaultCollectors() {
    return std::set<MetricCollectorType>{CPU_COLLECTOR, DISK_COLLECTOR, MEMORY_COLLECTOR, NETWORK_COLLECTOR};
}

bool MonitoringPlan::addMetric(MetricType metric) {
    if (hasMetric(metric)) {
        return false;
    }
    metricTypes.insert(metric);
    return true;
}

bool MonitoringPlan::hasMetric(MetricType metric) const {
    if (monitoringPlan.empty()) {
        return metricTypes.contains(metric);
    }
    return monitoringPlan.contains(metric);
}

std::string MonitoringPlan::toString() const {
    std::stringstream output;
    output << "MonitoringPlan:";
    if (monitoringPlan.empty()) {
        for (auto metric : metricTypes) {
            switch (metric) {
                case MetricType::CpuMetric: {
                    output << "cpu(True);";
                };
                case MetricType::DiskMetric: {
                    output << "disk(True);";
                };
                case MetricType::MemoryMetric: {
                    output << "memory(True);";
                };
                case MetricType::NetworkMetric: {
                    output << "network(True);";
                };
                case MetricType::RuntimeMetric: {
                    output << "runtimeMetrics(True);";
                };
                case MetricType::RegistrationMetric: {
                    output << "staticMetrics(True);";
                };
                default: {
                }
            }
        }
    } else {
        for (auto metric : monitoringPlan) {
            switch (metric.first) {
                case MetricType::CpuMetric: {
                    output << "cpu(True);";
                };
                case MetricType::DiskMetric: {
                    output << "disk(True);";
                };
                case MetricType::MemoryMetric: {
                    output << "memory(True);";
                };
                case MetricType::NetworkMetric: {
                    output << "network(True);";
                };
                case MetricType::RuntimeMetric: {
                    output << "runtimeMetrics(True);";
                };
                case MetricType::RegistrationMetric: {
                    output << "staticMetrics(True);";
                };
                default: {
                }
            }
        }
    }

    return output.str();
}

std::ostream& operator<<(std::ostream& strm, const MonitoringPlan& plan) { return strm << plan.toString(); }

//const std::map <MetricType, SchemaPtr>& MonitoringPlan::getMetricTypes() const { return monitoringPlan; }
//const std::set<MetricType>& MonitoringPlan::getMetricTypes() const { return metricTypes; }

const std::set<MetricType> MonitoringPlan::getMetricTypes() const {
    if(!(monitoringPlan.empty())) {
        std::set<MetricType> allMetricTypes;
        for (auto metric : monitoringPlan) {
            NES_DEBUG("MonitoringPlan: getMetricTypes: config is set:" + NES::toString(metric.first));
            allMetricTypes.insert(metric.first);
        }
        return allMetricTypes;
    }
    return metricTypes;
}

const std::set<MetricCollectorType> MonitoringPlan::getCollectorTypes() const {
    std::set<MetricCollectorType> output;
    for (auto mType : getMetricTypes()) {
        output.insert(MetricUtils::createCollectorTypeFromMetricType(mType));
    }
    return output;
}

}// namespace NES
