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

#include "Monitoring/Metrics/Gauge/CpuMetrics.hpp"
#include "Monitoring/Metrics/Gauge/DiskMetrics.hpp"
#include "Monitoring/Metrics/Gauge/MemoryMetrics.hpp"
#include "Monitoring/Metrics/Gauge/NetworkMetrics.hpp"
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Monitoring {
MonitoringPlan::MonitoringPlan(const std::map<MetricType, std::pair<SchemaPtr, uint64_t>>& metrics, std::list<uint64_t> cores)
    : monitoringPlan(metrics), cpuCores(cores) {
    NES_DEBUG("MonitoringPlan: Init with metrics of size " << metrics.size());
}

MonitoringPlanPtr MonitoringPlan::create(const std::map <MetricType, std::pair<SchemaPtr, uint64_t>>& monitoringPlan) {
    std::list<uint64_t> emptyList {};

    return std::shared_ptr<MonitoringPlan>(new MonitoringPlan(monitoringPlan, emptyList));
}

MonitoringPlanPtr MonitoringPlan::create(const std::map <MetricType, std::pair<SchemaPtr, uint64_t>>& monitoringPlan, std::list<uint64_t> cores) {
    return std::shared_ptr<MonitoringPlan>(new MonitoringPlan(monitoringPlan, cores));
}

MonitoringPlanPtr MonitoringPlan::defaultPlan() {
    std::map <MetricType, std::pair<SchemaPtr, uint64_t>> configuredMonitoringPlan;
    uint64_t sampleRate = 1000;
    std::pair<MetricType, std::pair<SchemaPtr, uint64_t>> tempPair;
    std::list<uint64_t> cores {};

    tempPair = std::make_pair(WrappedCpuMetrics, std::make_pair(CpuMetrics::getDefaultSchema(""), sampleRate));
    configuredMonitoringPlan.insert(tempPair);
    tempPair = std::make_pair(DiskMetric, std::make_pair(DiskMetrics::getDefaultSchema(""), sampleRate));
    configuredMonitoringPlan.insert(tempPair);
    tempPair = std::make_pair(MemoryMetric, std::make_pair(MemoryMetrics::getDefaultSchema(""), sampleRate));
    configuredMonitoringPlan.insert(tempPair);
    tempPair = std::make_pair(WrappedNetworkMetrics, std::make_pair(NetworkMetrics::getDefaultSchema(""), sampleRate));
    configuredMonitoringPlan.insert(tempPair);

    return MonitoringPlan::create(configuredMonitoringPlan, cores);
}

SchemaPtr MonitoringPlan::getSchema(MetricType metric) {
    if(monitoringPlan.contains(metric)) {
        return monitoringPlan.find(metric)->second.first;
    }
    return nullptr;
}

uint64_t MonitoringPlan::getSampleRate(MetricType metric) {
    return monitoringPlan.find(metric)->second.second;
}

std::list<uint64_t> MonitoringPlan::getCores() { return cpuCores; }

MonitoringPlanPtr MonitoringPlan::setSchemaJson(web::json::value& configuredMetrics) {
    std::map <MetricType, std::pair<SchemaPtr, uint64_t>> configuredMonitoringPlan;
    SchemaPtr schema;
    web::json::value attributesArray;
    std::list<std::string> attributesList;
    uint64_t sampleRate = 1000;
    std::list<uint64_t> cores {};
    std::pair<MetricType, std::pair<SchemaPtr, uint64_t>> tempPair;
    if (configuredMetrics["cpu"].is_object()) {
        if (configuredMetrics["cpu"]["sampleRate"].is_number()) {
            sampleRate = configuredMetrics["cpu"]["sampleRate"].as_integer();
        } else {
            sampleRate = 1000;
        }
        if (configuredMetrics["cpu"]["cores"].is_array()) {
            cores = MetricUtils::jsonArrayToIntegerList(configuredMetrics["cpu"]["cores"]);
        }
        attributesList = MetricUtils::jsonArrayToList(configuredMetrics["cpu"]["attributes"]);
        tempPair = std::make_pair(WrappedCpuMetrics, std::make_pair(CpuMetrics::createSchema("", attributesList), sampleRate));
        configuredMonitoringPlan.insert(tempPair);
    } if (configuredMetrics["disk"].is_object()) {
        if (configuredMetrics["disk"]["sampleRate"].is_number()) {
            sampleRate = configuredMetrics["disk"]["sampleRate"].as_integer();
        } else {
            sampleRate = 1000;
        }
        attributesList = MetricUtils::jsonArrayToList(configuredMetrics["disk"]["attributes"]);
        tempPair = std::make_pair(DiskMetric, std::make_pair(DiskMetrics::createSchema("", attributesList), sampleRate));
        configuredMonitoringPlan.insert(tempPair);
    } if (configuredMetrics["memory"].is_object()) {
        if (configuredMetrics["memory"]["sampleRate"].is_number()) {
            sampleRate = configuredMetrics["memory"]["sampleRate"].as_integer();
        } else {
            sampleRate = 1000;
        }
        attributesList = MetricUtils::jsonArrayToList(configuredMetrics["memory"]["attributes"]);
        tempPair = std::make_pair(MemoryMetric, std::make_pair(MemoryMetrics::createSchema("", attributesList), sampleRate));
        configuredMonitoringPlan.insert(tempPair);
    } if (configuredMetrics["network"].is_object()) {
        if (configuredMetrics["network"]["sampleRate"].is_number()) {
            sampleRate = configuredMetrics["network"]["sampleRate"].as_integer();

        } else {
            sampleRate = 1000;
        }
        attributesList = MetricUtils::jsonArrayToList(configuredMetrics["network"]["attributes"]);
        tempPair = std::make_pair(WrappedNetworkMetrics, std::make_pair(NetworkMetrics::createSchema("", attributesList), sampleRate));
        configuredMonitoringPlan.insert(tempPair);
    }

    return MonitoringPlan::create(configuredMonitoringPlan, cores);
}

std::set<MetricCollectorType> MonitoringPlan::defaultCollectors() {
    return std::set<MetricCollectorType>{CPU_COLLECTOR, DISK_COLLECTOR, MEMORY_COLLECTOR, NETWORK_COLLECTOR};
}

bool MonitoringPlan::hasMetric(MetricType metric) const {
    return monitoringPlan.contains(metric);
//    std::string metricStr = NES::Monitoring::toString(metric);
//    for (auto metricType : monitoringPlan) {
//        std::string metricTypeStr = NES::Monitoring::toString(metricType.first);
//        if (metricTypeStr ==  metricStr) {
//            return true;
//        }
//    }
//    return false;
}

bool MonitoringPlan::hasMetric(MetricType metric) const { return metricTypes.contains(metric); }

std::string MonitoringPlan::toString() const {
    std::stringstream output;
    output << "MonitoringPlan:";

    for (auto metric : monitoringPlan) {
        if (metric.first == MetricType::CpuMetric) {
            output << "cpu(True);";
        } else if (metric.first == MetricType::DiskMetric) {
            output << "disk(True);";
        } else if (metric.first == MetricType::MemoryMetric) {
            output << "memory(True);";
        } else if (metric.first == MetricType::NetworkMetric) {
            output << "network(True);";
        } else if (metric.first == MetricType::RuntimeMetric) {
            output << "runtimeMetrics(True);";
        } else if (metric.first == MetricType::RegistrationMetric) {
            output << "staticMetrics(True);";
        } else if (metric.first == MetricType::WrappedCpuMetrics) {
            output << "wrapped_cpu(True);";
        } else if (metric.first == MetricType::WrappedNetworkMetrics) {
            output << "wrapped_network(True);";
        }
    }
    return output.str();
}

std::ostream& operator<<(std::ostream& strm, const MonitoringPlan& plan) { return strm << plan.toString(); }

const std::set<MetricType> MonitoringPlan::getMetricTypes() const {

    std::set<MetricType> allMetricTypes;
    for (auto metric : monitoringPlan) {
        NES_DEBUG("MonitoringPlan: getMetricTypes: config is set:" + NES::Monitoring::toString(metric.first));
        allMetricTypes.insert(metric.first);
    }
    return allMetricTypes;
}

const std::set<MetricCollectorType> MonitoringPlan::getCollectorTypes() const {
    std::set<MetricCollectorType> output;
    for (auto mType : getMetricTypes()) {
        output.insert(MetricUtils::createCollectorTypeFromMetricType(mType));
    }
    return output;
}

}// namespace NES::Monitoring
