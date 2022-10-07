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

#ifndef NES_INCLUDE_MONITORING_UTIL_METRICUTILS_HPP_
#define NES_INCLUDE_MONITORING_UTIL_METRICUTILS_HPP_

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include "list"

namespace NES::Monitoring {

/**
 * @brief Pre-defined metrics used for NES internally.
 */
class MetricUtils {
  public:
    /**
     *
     * @param metricSchema
     * @param bufferSchema
     * @param prefix
     * @return
     */
    static bool validateFieldsInSchema(SchemaPtr metricSchema, SchemaPtr bufferSchema, uint64_t schemaIndex);

    /**
     * Converts a vector of metrics into json.
     * @param metrics
     * @return json of metrics
     */
    static web::json::value toJson(std::vector<MetricPtr> metrics);

    /**
     * Converts a map of metric types to metrics into json.
     * @param metrics
     * @return json of metrics
     */
    static web::json::value toJson(std::unordered_map<MetricType, std::shared_ptr<Metric>> metrics);

    /**
     * Converts a map of metric types to metrics into json.
     * @param metrics
     * @return json of metrics
     */
    static web::json::value toJson(StoredNodeMetricsPtr metrics);

    /**
     * Creates a metric collector from the corresponding type.
     * @param type
     * @return the metric collector shared ptr.
     */
    static MetricCollectorPtr createCollectorFromCollectorType(MetricCollectorType type);

    /**
     * Retrieves the schema from the according type.
     * @param type
     * @return the schema ptr
     */
    static SchemaPtr getSchemaFromCollectorType(MetricCollectorType type);

    /**
     * @brief Creates a metric from the according collector.
     * @param type
     * @return the metric as shared ptr.
     */
    static MetricPtr createMetricFromCollectorType(MetricCollectorType type);

    /**
     * @brief Creates a metric from the according collector.
     * @param type
     * @return the collector as type
     */
    static MetricCollectorType createCollectorTypeFromMetricType(MetricType type);

    /**
     * @brief Creates one string out of a given list of strings seperated by a given separator.
     * @param separator
     * @param list list of strings
     * @return string
     */
    static std::string listToString(const std::string& separator, const std::list<std::string>& list);

    /**
     * @brief Creates a tuple of a vector and a list. Vector has all attributes of a metric that a not configured.
     * The list consists of a given number of random attributes. Is just used for tests.
     * @param metric the metric we want to create the tuple for
     * @param numberOfAttributes number of attributes we want in the list
     * @return tuple
     */
    static std::tuple<std::vector<std::string>, std::list<std::string>> randomAttributes(std::string metric,
                                                                                         int numberOfAttributes);

    //TODO: Beschreibung
    static MetricType metricTypeFromSourceName(std::string sourceName);

    //TODO: Beschreibung
    static SchemaPtr defaultSchema(MetricType metricType);

    //TODO: Beschreibung
    static std::string createLogicalSourceName(MetricType metricType, SchemaPtr schema);

};
}// namespace NES::Monitoring

#endif// NES_INCLUDE_MONITORING_UTIL_METRICUTILS_HPP_