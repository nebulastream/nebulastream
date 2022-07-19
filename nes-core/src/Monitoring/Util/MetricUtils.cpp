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

namespace NES {
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
            NES_FATAL_ERROR("MetricUtils: Collector type not supported " << NES::toString(type));
        }
    }
    return nullptr;
}

SchemaPtr MetricUtils::getSchemaFromCollectorType(MetricCollectorType type) {
    switch (type) {
        case MetricCollectorType::CPU_COLLECTOR: return CpuMetrics::getSchema("");
        case MetricCollectorType::DISK_COLLECTOR: return DiskMetrics::getSchema("");
        case MetricCollectorType::MEMORY_COLLECTOR: return MemoryMetrics::getSchema("");
        case MetricCollectorType::NETWORK_COLLECTOR: return NetworkMetrics::getSchema("");
        default: {
            NES_FATAL_ERROR("MetricUtils: Collector type not supported " << NES::toString(type));
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
            NES_ERROR("MetricUtils: Metric type not supported " << NES::toString(type));
            return MetricCollectorType::INVALID;
        }
    }
}

std::map <MetricType, std::list<std::string>> MetricUtils::parseMonitoringConfigStringToMap(std::string rawConfigString) {
    std::map <MetricType, std::list<std::string>> MonitoringConfigurationMap;
    std::string delimiter(" - ");


    // erase first 1 characters
    if (rawConfigString.substr(0, 3) == delimiter) {
        rawConfigString.erase(0, 3);
    } else {
        //throw exception
        std::cout << "String has the wrong format";
    }

    std::list<std::string> tokenList;

    size_t pos = 0;
    std::string token;
    std::list<std::string>::iterator i;
    i = tokenList.begin();
    while ((pos = rawConfigString.find(delimiter)) != std::string::npos) {
        token = rawConfigString.substr(0, pos);
        rawConfigString.erase(0, pos + delimiter.length());
        tokenList.insert(i, token);
        ++i;
    }
    tokenList.insert(i, rawConfigString);

    std::string metricType;
    std::string attributes;
    std::list<std::string> listOfAttributes;
    std::string sampleRate;
    std::list<std::string>::iterator j;
    for (i = tokenList.begin(); i != tokenList.end(); ++i) {
        delimiter = ": ";
        pos = i->find(delimiter);
        metricType = i->substr(0, pos);
        i->erase(0, pos + delimiter.length());

        delimiter = "attributes: \"";
        i->erase(0, delimiter.length());

        delimiter = "\" ";
        pos = i->find(delimiter);
        attributes = i->substr(0, pos);
        i->erase(0, pos + delimiter.length());

        delimiter= "sampleRate: ";
        i->erase(0, delimiter.length());

        delimiter = " ";
        pos = i->find(delimiter);
        sampleRate = i->substr(0, pos);

        attributes.insert(0, sampleRate + ", ");

        //Attribute String zu einer Liste parsen
        delimiter = ", ";
        j = listOfAttributes.begin();
        while ((pos = attributes.find(delimiter)) != std::string::npos) {
            token = attributes.substr(0, pos);
            attributes.erase(0, pos + delimiter.length());
            listOfAttributes.insert(j, token);
            ++j;
        }
        listOfAttributes.insert(j, attributes);

        MonitoringConfigurationMap[NES::parse(metricType)] = listOfAttributes;
        listOfAttributes.clear();

    }

    return MonitoringConfigurationMap;
}

//web::json::value MetricUtils::ConfigMapToJson(std::map <MetricType, std::list<std::string>> mapConfigurationMonitoring) {
//    web::json::value configurationMonitoringJson{};
//    std::map <MetricType, std::list<std::string>>::iterator i;
//    for (i = mapConfigurationMonitoring.begin(); i != mapConfigurationMonitoring.end(); i++) {
//        configurationMonitoringJson[i->first] =  web::jsoi->second;
//    }
//    return configurationMonitoringJson;
//}

}// namespace NES