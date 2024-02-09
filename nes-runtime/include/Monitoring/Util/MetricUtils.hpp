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

#ifndef NES_RUNTIME_INCLUDE_MONITORING_UTIL_METRICUTILS_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_UTIL_METRICUTILS_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Util/MetricCollectorType.hpp>

namespace NES {

namespace Configurations {
class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;
}// namespace Configurations

namespace Monitoring {

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
    static nlohmann::json toJson(std::vector<MetricPtr> metrics);

    /**
    * Converts a map of metric types to metrics into json.
    * @param metrics
    * @return json of metrics
    */
    static nlohmann::json toJson(std::unordered_map<MetricType, std::shared_ptr<Metric>> metrics);

    /**
    * Converts a map of metric types to metrics into json.
    * @param metrics
    * @return json of metrics
    */
    static nlohmann::json toJson(StoredNodeMetricsPtr metrics);

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
    static Configurations::SchemaTypePtr getSchemaFromCollectorType(MetricCollectorType type);

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
};

}// namespace Monitoring
}// namespace NES

#endif// NES_RUNTIME_INCLUDE_MONITORING_UTIL_METRICUTILS_HPP_
