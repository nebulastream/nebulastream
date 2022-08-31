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
#include <map>
#include <list>

namespace NES {

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
     *
     * @param rawConfigString The raw string that got parsed from the config Yaml-File
     * @return a map with the configured metrics types and their attributes
     */

//    static std::map<MetricType, std::list<std::string>> parseMonitoringConfigStringToMap(std::string rawConfigString);

    static SchemaPtr defaultSchema(MetricType metricType);

    static std::string createLogicalSourceName(MetricType metricType, SchemaPtr schema);
    /**
     *
     * @param mapConfigurationMonitoring
     * @return
     */
//    static web::json::value ConfigMapToJson(std::map <MetricType, std::list<std::string>> mapConfigurationMonitoring);

    static std::list<std::string> jsonArrayToList(web::json::value jsonAttributes);

    static web::json::value parseMonitoringConfigStringToJson(std::string rawConfigString);

    static std::string listToString(const std::string& seperator, const std::list<std::string>& list);

    static std::tuple<std::vector<std::string>, std::list<std::string>> randomAttributes(std::string metric, int numberOfAttributes);


};

}// namespace NES

#endif// NES_INCLUDE_MONITORING_UTIL_METRICUTILS_HPP_