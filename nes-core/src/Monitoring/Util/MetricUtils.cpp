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
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>

namespace NES::Monitoring {
bool MetricUtils::validateFieldsInSchema(SchemaPtr metricSchema, SchemaPtr bufferSchema, uint64_t i) {
    if (i >= bufferSchema->getSize()) {
        return false;
    }

    auto hasName = Util::endsWith(bufferSchema->fields[i]->getName(), metricSchema->get(0)->getName());
    auto hasLastField = Util::endsWith(bufferSchema->fields[i + metricSchema->getSize() - 1]->getName(),
                                       metricSchema->get(metricSchema->getSize() - 1)->getName());

    return hasName && hasLastField;
}

web::json::value MetricUtils::toJson(std::vector<MetricPtr> metrics) {
    web::json::value metricsJson{};
    for (const auto& metric : metrics) {
        auto jMetric = asJson(metric);
        metricsJson[toString(metric->getMetricType())] = jMetric;
    }
    return metricsJson;
}

web::json::value MetricUtils::toJson(std::unordered_map<MetricType, std::shared_ptr<Metric>> metrics) {
    web::json::value metricsJson{};
    for (const auto& metric : metrics) {
        web::json::value jMetric = asJson(metric.second);
        metricsJson[toString(metric.second->getMetricType())] = jMetric;
    }
    return metricsJson;
}

web::json::value MetricUtils::toJson(StoredNodeMetricsPtr metrics) {
    web::json::value metricsJson{};
    for (auto metricTypeEntry : *metrics.get()) {
        std::shared_ptr<std::vector<TimestampMetricPtr>> metricVec = metricTypeEntry.second;
        web::json::value arr{};
        int i = 0;
        for (const auto& metric : *metricTypeEntry.second.get()) {
            web::json::value jsonMetricVal{};
            uint64_t timestamp = metric->first;
            MetricPtr metricVal = metric->second;
            web::json::value jMetric = asJson(metricVal);
            jsonMetricVal["timestamp"] = web::json::value::number(timestamp);
            jsonMetricVal["value"] = jMetric;
            arr[i++] = jsonMetricVal;
        }
        metricsJson[toString(metricTypeEntry.first)] = arr;
    }
    return metricsJson;
}

MetricCollectorPtr MetricUtils::createCollectorFromCollectorType(MetricCollectorType type) {
    switch (type) {
        case MetricCollectorType::CPU_COLLECTOR: return std::make_shared<CpuCollector>();
        case MetricCollectorType::DISK_COLLECTOR: return std::make_shared<DiskCollector>();
        case MetricCollectorType::MEMORY_COLLECTOR: return std::make_shared<MemoryCollector>();
        case MetricCollectorType::NETWORK_COLLECTOR: return std::make_shared<NetworkCollector>();
        default: NES_FATAL_ERROR("MetricUtils: Not supported collector type " << toString(type));
    }
    return nullptr;
}

MetricPtr MetricUtils::createMetricFromCollectorType(MetricCollectorType type) {
    switch (type) {
        case MetricCollectorType::CPU_COLLECTOR: return std::make_shared<Metric>(CpuMetricsWrapper{}, WrappedCpuMetrics);
        case MetricCollectorType::DISK_COLLECTOR: return std::make_shared<Metric>(DiskMetrics{}, DiskMetric);
        case MetricCollectorType::MEMORY_COLLECTOR: return std::make_shared<Metric>(MemoryMetrics{}, MemoryMetric);
        case MetricCollectorType::NETWORK_COLLECTOR:
            return std::make_shared<Metric>(NetworkMetricsWrapper{}, WrappedNetworkMetrics);
        default: {
            NES_FATAL_ERROR("MetricUtils: Collector type not supported " << NES::Monitoring::toString(type));
        }
    }
    return nullptr;
}

SchemaPtr MetricUtils::getSchemaFromCollectorType(MetricCollectorType type) {
    switch (type) {
        case MetricCollectorType::CPU_COLLECTOR: return CpuMetrics::getDefaultSchema("");
        case MetricCollectorType::DISK_COLLECTOR: return DiskMetrics::getDefaultSchema("");
        case MetricCollectorType::MEMORY_COLLECTOR: return MemoryMetrics::getDefaultSchema("");
        case MetricCollectorType::NETWORK_COLLECTOR: return NetworkMetrics::getDefaultSchema("");
        default: {
            NES_FATAL_ERROR("MetricUtils: Collector type not supported " << NES::Monitoring::toString(type));
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
            NES_ERROR("MetricUtils: Metric type not supported " << NES::Monitoring::toString(type));
            return MetricCollectorType::INVALID;
        }
    }
}

std::string MetricUtils::listToString(const std::string& separator, const std::list<std::string>& list) {
    std::string string;
    auto lengthSeperator = separator.size();
    for (const std::string& listItem : list) {
        string += listItem + separator;
    }
    string = string.substr(0, string.size()-lengthSeperator);

    return string;
}

std::tuple<std::vector<std::string>, std::list<std::string>> MetricUtils::randomAttributes(std::string metric, int numberOfAttributes) {
    std::vector<std::string> attributesVector;

    if (metric == "CpuMetric") {
        attributesVector = CpuMetrics::getAttributesVector();
    } else if (metric == "DiskMetric") {
        attributesVector = DiskMetrics::getAttributesVector();
    } else if (metric == "MemoryMetric") {
        attributesVector = MemoryMetrics::getAttributesVector();
    } else if (metric == "NetworkMetric") {
        attributesVector = NetworkMetrics::getAttributesVector();
    } else {
        // TODO through exception
    }

    std::list<std::string> configuredAttributes;

    int random;
    for (int i = 0; i < numberOfAttributes; i++) {
        random = std::rand() % attributesVector.size();
        configuredAttributes.push_back(attributesVector[random]);
        attributesVector.erase(attributesVector.begin()+random);
    }

    std::tuple<std::vector<std::string>, std::list<std::string>> returnTuple(attributesVector, configuredAttributes);
    return returnTuple;
}

MetricType MetricUtils::metricTypeFromSourceName(std::string sourceName) {
    MetricType metricType;
    if (sourceName.substr(0, 4) == "disk"){
        metricType = DiskMetric;
    } else if (sourceName.substr(0, 6) == "memory") {
        metricType = MemoryMetric;
    } else if (sourceName.substr(0, 11) == "wrapped_cpu") {
        metricType = WrappedCpuMetrics;
    } else if (sourceName.substr(0, 15) == "wrapped_network") {
        metricType = WrappedNetworkMetrics;
    } else {
        //TODO: other source names
        NES_ERROR("MetricUtils: metricTypeFromSourceName: MetricType cannot be defined from sourceName")
        metricType = UnknownMetric;
    }

    return metricType;
}

SchemaPtr MetricUtils::defaultSchema(MetricType metricType) {
    SchemaPtr defaultSchema;
    if (metricType == CpuMetric || metricType == WrappedCpuMetrics) {
        defaultSchema = CpuMetrics::getDefaultSchema("");
    } else if (metricType == NetworkMetric || metricType == WrappedNetworkMetrics) {
        defaultSchema = NetworkMetrics::getDefaultSchema("");
    } else if (metricType == DiskMetric) {
        defaultSchema = DiskMetrics::getDefaultSchema("");
    } else if (metricType == MemoryMetric) {
        defaultSchema = MemoryMetrics::getDefaultSchema("");
    }

    return defaultSchema;
}

std::string MetricUtils::createLogicalSourceName(MetricType metricType, SchemaPtr schema) {
    std::string logicalSourceName = NES::Monitoring::toString(metricType) + "_" + schema->toStringForLogicalSourceName();
    return logicalSourceName;
}
}// namespace NES::Monitoring