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

MonitoringPlan::MonitoringPlan(const std::map<MetricType, SchemaPtr>& metrics) : monitoringPlan(metrics) {
    NES_DEBUG("MonitoringPlan: Init with metrics of size " << metrics.size());
}

MonitoringPlanPtr MonitoringPlan::create(const std::set<MetricType>& metrics) {
    return std::shared_ptr<MonitoringPlan>(new MonitoringPlan(metrics));
}

MonitoringPlanPtr MonitoringPlan::create(const std::map <MetricType, SchemaPtr>& monitoringPlan) {
    return std::shared_ptr<MonitoringPlan>(new MonitoringPlan(monitoringPlan));
}

//set the MonitoringPlan for the Yaml Konfiguration
MonitoringPlanPtr MonitoringPlan::setSchema(const std::map <MetricType, std::list<std::string>>& configuredMetrics) {
    std::map <MetricType, SchemaPtr> configuredMonitoringPlan;
    SchemaPtr schema;
    if (configuredMetrics.count(CpuMetric) > 0) {
        auto it = configuredMetrics.find(CpuMetric);
        configuredMonitoringPlan.insert(std::pair<MetricType,
                                                  SchemaPtr>(CpuMetric, CpuMetrics::createSchema("", it->second)));
        //call setSchema Function for the Metric
        //insert MetricType and Schema into configuredMonitoringPlan
    } if (configuredMetrics.count(DiskMetric) > 0) {
        auto it = configuredMetrics.find(DiskMetric);
        configuredMonitoringPlan.insert(std::pair<MetricType,
                                                  SchemaPtr>(DiskMetric, DiskMetrics::createSchema("", it->second)));
    } if (configuredMetrics.count(MemoryMetric) > 0) {
        auto it = configuredMetrics.find(MemoryMetric);
        configuredMonitoringPlan.insert(std::pair<MetricType,
                                                  SchemaPtr>(MemoryMetric, MemoryMetrics::createSchema("", it->second)));
        //call setSchema Function for the Metric
        //insert MetricType and Schema into configuredMonitoringPlan
    } if (configuredMetrics.count(NetworkMetric) > 0) {
        auto it = configuredMetrics.find(NetworkMetric);
        configuredMonitoringPlan.insert(std::pair<MetricType,
                                                  SchemaPtr>(NetworkMetric, NetworkMetrics::createSchema("", it->second)));
    } if (configuredMetrics.count(RegistrationMetric) > 0) {
        auto it = configuredMetrics.find(RegistrationMetric);
        configuredMonitoringPlan.insert(std::pair<MetricType,
                                                  SchemaPtr>(RegistrationMetric, RegistrationMetrics::createSchema("", it->second)));
        //call setSchema Function for the Metric
        //insert MetricType and Schema into configuredMonitoringPlan
    } if (configuredMetrics.count(RuntimeMetric) > 0) {
        auto it = configuredMetrics.find(RuntimeMetric);
        configuredMonitoringPlan.insert(std::pair<MetricType,
                                                  SchemaPtr>(RuntimeMetric, DiskMetrics::createSchema("", it->second)));
    }

    return MonitoringPlan::create(configuredMonitoringPlan);
}

//MonitoringPlanPtr MonitoringPlan::setSchemaJson(web::json::value& configuredMetrics) {
//    std::map <MetricType, SchemaPtr> configuredMonitoringPlan;
//    SchemaPtr schema;
//    std::vector<std::string> attributesVector;
//    web::json::value jsonArray;
//    if (configuredMetrics["cpu"]["sampleRate"].is_number()) {
//        //call setSchema Function for the Metric
//        //insert MetricType and Schema into configuredMonitoringPlan
//    } if (configuredMetrics["disk"]["sampleRate"].is_number()) {
////        attributesVector = configuredMetrics["disk"]["attributes"].as_array();
//        configuredMonitoringPlan.insert(std::pair<MetricType,
//                                                  SchemaPtr>(DiskMetric, DiskMetrics::createSchemaJson("", configuredMetrics.get("attributes"))));
//    }
//    //do the same for all Metrics
//
//    return MonitoringPlan::create(configuredMonitoringPlan);
//}

SchemaPtr MonitoringPlan::getSchema(MetricType metric) {
    return monitoringPlan.find(metric)->second;
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

bool MonitoringPlan::hasMetric(MetricType metric) const { return metricTypes.contains(metric); }

std::string MonitoringPlan::toString() const {
    std::stringstream output;
    output << "MonitoringPlan:";

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
    return output.str();
}

std::ostream& operator<<(std::ostream& strm, const MonitoringPlan& plan) { return strm << plan.toString(); }

const std::set<MetricType>& MonitoringPlan::getMetricTypes() const { return metricTypes; }

const std::set<MetricCollectorType> MonitoringPlan::getCollectorTypes() const {
    std::set<MetricCollectorType> output;
    for (auto mType : getMetricTypes()) {
        output.insert(MetricUtils::createCollectorTypeFromMetricType(mType));
    }
    return output;
}

}// namespace NES
