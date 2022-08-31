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
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Metrics/Wrapper/NetworkMetricsWrapper.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>
#include <Monitoring/Metrics/MetricType.hpp>
#include <Monitoring/Metrics/Gauge/RuntimeMetrics.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>

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
    NES_DEBUG("MetricUtils: Vector get used!");
    for (const auto& metric : metrics) {
        auto jMetric = asJson(metric);
        metricsJson[toString(metric->getMetricType())] = jMetric;
    }
    return metricsJson;
}

web::json::value MetricUtils::toJson(std::unordered_map<MetricType, std::shared_ptr<Metric>> metrics) {
    NES_DEBUG("MetricUtils: Unordered map get used!");
    web::json::value metricsJson{};
    for (const auto& metric : metrics) {
        web::json::value jMetric = asJson(metric.second);
        metricsJson[toString(metric.second->getMetricType())] = jMetric;
    }
    return metricsJson;
}

web::json::value MetricUtils::toJson(StoredNodeMetricsPtr metrics) {
    NES_DEBUG("MetricUtils: SotredNodeMetricsPtr get used!");
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
        case MetricCollectorType::CPU_COLLECTOR: return CpuMetrics::getDefaultSchema("");
        case MetricCollectorType::DISK_COLLECTOR: return DiskMetrics::getDefaultSchema("");
        case MetricCollectorType::MEMORY_COLLECTOR: return MemoryMetrics::getDefaultSchema("");
        case MetricCollectorType::NETWORK_COLLECTOR: return NetworkMetrics::getDefaultSchema("");
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

//std::map <MetricType, std::list<std::string>> MetricUtils::parseMonitoringConfigStringToMap(std::string rawConfigString) {
//    // init map to save everything
//    std::map <MetricType, std::list<std::string>> monitoringConfigurationMap;
//
//    // argument for splitting the string
//    std::string delimiter(" - ");
//
//    // erase first 3 characters from the raw string and check, if the string has the right format in the beginning
//    if (rawConfigString.substr(0, 3) == delimiter) {
//        rawConfigString.erase(0, 3);
//    } else {
//        //throw exception
//        std::cout << "String has the wrong format";
//    }
//    // list to save strings for each metric
//    std::list<std::string> tokenList;
//
//    size_t pos = 0;
//    std::string token;
//    std::list<std::string>::iterator i;
//    i = tokenList.begin();
//
//    // split rawString into n different strings;
//    // each string will represent the configuration of one metric
//    while ((pos = rawConfigString.find(delimiter)) != std::string::npos) {
//        token = rawConfigString.substr(0, pos);
//        rawConfigString.erase(0, pos + delimiter.length());
//        tokenList.insert(i, token);
//        ++i;
//    }
//    tokenList.insert(i, rawConfigString);
//
//    std::string metricType;
//    std::string attributes;
//    std::list<std::string> listOfAttributes;
//    std::string sampleRate;
//    std::list<std::string>::iterator j;
//    // split the metric configuration strings into the form of the map
//    // key: metricType
//    // value: list of strings including all attributes of metric and the sample rate
//    for (i = tokenList.begin(); i != tokenList.end(); ++i) {
//        delimiter = ": ";
//        pos = i->find(delimiter);
//        metricType = i->substr(0, pos);
//        i->erase(0, pos + delimiter.length());
//
//        delimiter = "attributes: \"";
//        i->erase(0, delimiter.length());
//
//        delimiter = "\" ";
//        pos = i->find(delimiter);
//        attributes = i->substr(0, pos);
//        i->erase(0, pos + delimiter.length());
//
//        delimiter= "sampleRate: ";
//        i->erase(0, delimiter.length());
//
//        delimiter = " ";
//        pos = i->find(delimiter);
//        sampleRate = i->substr(0, pos);
//
//        attributes.insert(0, sampleRate + ", ");
//
//        // parse attribute string to list of strings
//        delimiter = ", ";
//        j = listOfAttributes.begin();
//        while ((pos = attributes.find(delimiter)) != std::string::npos) {
//            token = attributes.substr(0, pos);
//            attributes.erase(0, pos + delimiter.length());
//            listOfAttributes.insert(j, token);
//            ++j;
//        }
//        listOfAttributes.insert(j, attributes);
//
//        monitoringConfigurationMap[NES::parse(metricType)] = listOfAttributes;
//        listOfAttributes.clear();
//
//    }
//
//    return monitoringConfigurationMap;
//}

web::json::value MetricUtils::parseMonitoringConfigStringToJson(std::string rawConfigString) {
    // init json to save everything
    web::json::value monitoringConfigurationJson;
    // argument for splitting the string
    std::string delimiter(" - ");

    // erase first 3 characters from the raw string and check, if the string has the right format in the beginning
    if (rawConfigString.substr(0, 3) == delimiter) {
        rawConfigString.erase(0, 3);
    } else {
        //throw exception
        std::cout << "String has the wrong format";
    }
    // list to save strings for each metric
    std::list<std::string> tokenList;

    size_t pos = 0;
    std::string token;
    std::list<std::string>::iterator i;
    i = tokenList.begin();

    // split rawString into n different strings;
    // each string will represent the configuration of one metric
    while ((pos = rawConfigString.find(delimiter)) != std::string::npos) {
        token = rawConfigString.substr(0, pos);
        rawConfigString.erase(0, pos + delimiter.length());
        tokenList.insert(i, token);
        ++i;
    }
    tokenList.insert(i, rawConfigString);

    std::string metricType;
    std::string attributes;
    std::vector<web::json::value> vectorAttributes;
    std::string sampleRate;
    std::list<std::string>::iterator j;
    // split the metric configuration strings and parse them to json
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

        monitoringConfigurationJson[metricType]["sampleRate"] = web::json::value::number(std::stoi(sampleRate));

        // parse attribute string to list of strings
        delimiter = ", ";
        while ((pos = attributes.find(delimiter)) != std::string::npos) {
            token = attributes.substr(0, pos);
            attributes.erase(0, pos + delimiter.length());
            vectorAttributes.push_back(web::json::value::string(token));

        }
        vectorAttributes.push_back(web::json::value::string(attributes));
        monitoringConfigurationJson[metricType]["attributes"] = web::json::value::array(vectorAttributes);
        vectorAttributes.clear();
    }

    return monitoringConfigurationJson;
}

//web::json::value MetricUtils::ConfigMapToJson(std::map <MetricType, std::list<std::string>> mapConfigurationMonitoring) {
//    web::json::value configurationMonitoringJson;
//    web::json::value temp;
//    std::map <MetricType, std::list<std::string>>::iterator i;
//    std::list<std::string>::iterator j;
//
//    //eventuell zu uint casten
//    int sampleRate;
//    std::vector<web::json::value> vectorAttributes;
//    for (i = mapConfigurationMonitoring.begin(); i != mapConfigurationMonitoring.end(); ++i) {
//        i->second.sort();
//        sampleRate = std::stoi(i->second.front());
//        i->second.pop_front();
//        configurationMonitoringJson[toString(i->first)]["sampleRate"] = web::json::value::number(sampleRate);
//        for (j = i->second.begin(); j != i->second.end(); ++j) {
//            vectorAttributes.push_back(web::json::value::string(*j));
//            std::cout << "String has the wrong format";
//        }
//        configurationMonitoringJson[toString(i->first)]["attributes"] = web::json::value::array(vectorAttributes);
//        vectorAttributes.clear();
//    }
//
//    return configurationMonitoringJson;
//}

std::string MetricUtils::listToString(const std::string& seperator, const std::list<std::string>& list) {
    std::string string;
    auto lengthSeperator = seperator.size();
    for (const std::string& listItem : list) {
        string += listItem + seperator;
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
    } else if (metric == "RegistrationMetric") {
        attributesVector = RegistrationMetrics::getAttributesVector();
    } else if (metric == "RuntimeMetric") {
        attributesVector = RuntimeMetrics::getAttributesVector();
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
std::list<std::string> MetricUtils:: jsonArrayToList(web::json::value jsonAttributes) {
    std::list<std::string> attributesList;
    int i;
    auto arrayLength = jsonAttributes.size();
    for (i = 0; i < static_cast<int>(arrayLength); i++) {
        attributesList.push_front(jsonAttributes[i].as_string());
    }

    return attributesList;
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
    std::string logicalSourceName = NES::toString(metricType) + "_" + schema->toString();
    return logicalSourceName;
}
}// namespace NES