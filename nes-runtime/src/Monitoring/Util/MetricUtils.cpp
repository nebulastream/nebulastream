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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include <Monitoring/MetricCollectors/MemoryCollector.hpp>
#include <Monitoring/MetricCollectors/NetworkCollector.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {

bool MetricUtils::validateFieldsInSchema(SchemaPtr metricSchema, SchemaPtr bufferSchema, uint64_t i) {
    if (i >= bufferSchema->getSize()) {
        return false;
    }

    auto hasName = NES::Util::endsWith(bufferSchema->fields[i]->getName(), metricSchema->get(0)->getName());
    auto hasLastField = NES::Util::endsWith(bufferSchema->fields[i + metricSchema->getSize() - 1]->getName(),
                                            metricSchema->get(metricSchema->getSize() - 1)->getName());

    return hasName && hasLastField;
}

nlohmann::json MetricUtils::toJson(std::vector<MetricPtr> metrics) {
    nlohmann::json metricsJson{};
    for (const auto& metric : metrics) {
        auto jMetric = asJson(metric);
        metricsJson[std::string(magic_enum::enum_name(metric->getMetricType()))] = jMetric;
    }
    return metricsJson;
}

nlohmann::json MetricUtils::toJson(std::unordered_map<MetricType, std::shared_ptr<Metric>> metrics) {
    nlohmann::json metricsJson{};
    for (const auto& metric : metrics) {
        nlohmann::json jMetric = asJson(metric.second);
        metricsJson[std::string(magic_enum::enum_name(metric.second->getMetricType()))] = jMetric;
    }
    return metricsJson;
}

nlohmann::json MetricUtils::toJson(StoredNodeMetricsPtr metrics) {
    nlohmann::json metricsJson{};
    for (auto metricTypeEntry : *metrics.get()) {
        std::shared_ptr<std::vector<TimestampMetricPtr>> metricVec = metricTypeEntry.second;
        nlohmann::json arr{};
        int i = 0;
        for (const auto& metric : *metricTypeEntry.second.get()) {
            nlohmann::json jsonMetricVal{};
            uint64_t timestamp = metric->first;
            MetricPtr metricVal = metric->second;
            nlohmann::json jMetric = asJson(metricVal);
            jsonMetricVal["timestamp"] = timestamp;
            jsonMetricVal["value"] = jMetric;
            arr[i++] = jsonMetricVal;
        }
        metricsJson[std::string(magic_enum::enum_name(metricTypeEntry.first))] = arr;
    }
    return metricsJson;
}

MetricCollectorPtr MetricUtils::createCollectorFromCollectorType(MetricCollectorType type) {
    switch (type) {
        case MetricCollectorType::CPU_COLLECTOR: return std::make_shared<CpuCollector>();
        case MetricCollectorType::DISK_COLLECTOR: return std::make_shared<DiskCollector>();
        case MetricCollectorType::MEMORY_COLLECTOR: return std::make_shared<MemoryCollector>();
        case MetricCollectorType::NETWORK_COLLECTOR: return std::make_shared<NetworkCollector>();
        default: NES_FATAL_ERROR("MetricUtils: Not supported collector type {}", std::string(magic_enum::enum_name(type)));
    }
    return nullptr;
}

MetricPtr MetricUtils::createMetricFromCollectorType(MetricCollectorType type) {
    switch (type) {
        case MetricCollectorType::CPU_COLLECTOR:
            return std::make_shared<Metric>(CpuMetricsWrapper{}, MetricType::WrappedCpuMetrics);
        case MetricCollectorType::DISK_COLLECTOR: return std::make_shared<Metric>(DiskMetrics{}, MetricType::DiskMetric);
        case MetricCollectorType::MEMORY_COLLECTOR: return std::make_shared<Metric>(MemoryMetrics{}, MetricType::MemoryMetric);
        case MetricCollectorType::NETWORK_COLLECTOR:
            return std::make_shared<Metric>(NetworkMetricsWrapper{}, MetricType::WrappedNetworkMetrics);
        default: {
            NES_FATAL_ERROR("MetricUtils: Collector type not supported {}", std::string(magic_enum::enum_name(type)));
        }
    }
    return nullptr;
}

Configurations::SchemaTypePtr MetricUtils::getSchemaFromCollectorType(MetricCollectorType type) {
    switch (type) {
        case MetricCollectorType::CPU_COLLECTOR: return CpuMetrics::getSchemaType("");
        case MetricCollectorType::DISK_COLLECTOR: return DiskMetrics::getSchemaType("");
        case MetricCollectorType::MEMORY_COLLECTOR: return MemoryMetrics::getSchemaType("");
        case MetricCollectorType::NETWORK_COLLECTOR: return NetworkMetrics::getSchemaType("");
        default: {
            NES_FATAL_ERROR("MetricUtils: Collector type not supported {}", std::string(magic_enum::enum_name(type)));
        }
    }
    return nullptr;
}

MetricCollectorType MetricUtils::createCollectorTypeFromMetricType(MetricType type) {
    switch (type) {
        case MetricType::CpuMetric: return MetricCollectorType::CPU_COLLECTOR;
        case MetricType::WrappedCpuMetrics: return MetricCollectorType::CPU_COLLECTOR;
        case MetricType::DiskMetric: return MetricCollectorType::DISK_COLLECTOR;
        case MetricType::MemoryMetric: return MetricCollectorType::MEMORY_COLLECTOR;
        case MetricType::NetworkMetric: return MetricCollectorType::NETWORK_COLLECTOR;
        case MetricType::WrappedNetworkMetrics: return MetricCollectorType::NETWORK_COLLECTOR;
        default: {
            NES_ERROR("MetricUtils: Metric type not supported {}", std::string(magic_enum::enum_name(type)));
            return MetricCollectorType::INVALID;
        }
    }
}

}// namespace NES::Monitoring