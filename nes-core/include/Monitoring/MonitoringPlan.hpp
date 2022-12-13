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

#ifndef NES_INCLUDE_MONITORING_METRICS_MONITORING_PLAN_HPP_
#define NES_INCLUDE_MONITORING_METRICS_MONITORING_PLAN_HPP_

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Monitoring/Metrics/MetricType.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <memory>
#include <set>
#include <string>
#include <map>
#include <list>

namespace NES::Monitoring {

/**
* @brief The MonitoringPlan is a config class to represent what metrics shall be collected and how.
*/
class MonitoringPlan {
  public:
//    static MonitoringPlanPtr create(const std::set<MetricType>& metrics);

    static MonitoringPlanPtr create(const std::map <MetricType, std::pair<SchemaPtr, uint64_t>>& metrics);
    static MonitoringPlanPtr create(const std::map <MetricType, std::pair<SchemaPtr, uint64_t>>& metrics, std::list<uint64_t> cores);
    static MonitoringPlanPtr defaultPlan();

    /**
     * @brief Gets the schema for a metric type
     * @param metric
     * @return Ptr to the schema
    */
    SchemaPtr getSchema(MetricType metric);

    /**
     * @brief Gets the sample rate for a metric type
     * @param metric
     * @return sample rate
    */
    uint64_t getSampleRate(MetricType metric);

    /**
     * @brief Creates a monitoring plan for a given configuration of the monitoring
     * @param configuredMetrics: configuration of the monitoring
     * @return Ptr to the monitoring plan
    */
    static MonitoringPlanPtr setSchemaJson(web::json::value& configuredMetrics);

    /**
     * @brief Gets a list of cpu core numbers that have to be monitored
     * @return List of cores
    */
    std::list<uint64_t> getCores();
    /**
     * @brief Returns the default collectors of the plan.
     * @return A set of collectors.
     */
    static std::set<MetricCollectorType> defaultCollectors();

    /**
     * @brief Add a specific metric to the plan
     * @param metric
    */
//    bool addMetric(MetricType metric);

    /**
     * @brief Checks if a metric is part of the MonitoringPlan
     * @param metric
     * @return true if contained, else false
    */
    [[nodiscard]] bool hasMetric(MetricType metric) const;

    /**
     * @brief Returns a string representation of the plan
     * @return The string representation
    */
    [[nodiscard]] std::string toString() const;

    /**
     * @brief Returns the MetricType objects that represent the plan.
     * @return A set of metric type objects.
    */
    [[nodiscard]] const std::set<MetricType> getMetricTypes() const;

    /**
     * @brief Returns the MetricType objects that represent the plan.
     * @return A set of metric type objects.
    */
    [[nodiscard]] const std::set<MetricCollectorType> getCollectorTypes() const;

    friend std::ostream& operator<<(std::ostream&, const MonitoringPlan&);

  private:
//    explicit MonitoringPlan(const std::set<MetricType>& metrics);
    explicit MonitoringPlan(const std::map <MetricType, std::pair<SchemaPtr, uint64_t>>& metrics, std::list<uint64_t> cores);

    //enum defined in SerializableDataType.proto
//    std::set<MetricType> metricTypes;
    std::map <MetricType, std::pair<SchemaPtr, uint64_t>> monitoringPlan;
    std::list<uint64_t> cpuCores = {};
};

}// namespace NES::Monitoring

#endif// NES_INCLUDE_MONITORING_METRICS_MONITORING_PLAN_HPP_