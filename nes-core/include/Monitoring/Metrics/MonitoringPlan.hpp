/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace NES {

class MetricCatalog;
class MetricGroup;
class GroupedMetricValues;
class SerializableMonitoringPlan;
class MonitoringPlan;
class MetricCatalog;

using MetricCatalogPtr = std::shared_ptr<MetricCatalog>;
using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;
using MetricGroupPtr = std::shared_ptr<MetricGroup>;

/**
 * @brief The MonitoringPlan is a config class to represent what metrics shall be collected and how.
 */
class MonitoringPlan {
  public:
    static MonitoringPlanPtr create(const std::vector<MetricValueType>& metrics);
    static MonitoringPlanPtr create(const SerializableMonitoringPlan& shippable);
    static MonitoringPlanPtr DefaultPlan();

    /**
     * @brief Add a specific metric to the plan
     * @param metric
     */
    bool addMetric(MetricValueType metric);

    /**
     * @brief Add an array of metrics to the plan
     * @param metrics
     */
    void addMetrics(const std::vector<MetricValueType>& metrics);

    /**
     * @brief Checks if a metric is part of the MonitoringPlan
     * @param metric
     * @return true if contained, else false
     */
    [[nodiscard]] bool hasMetric(MetricValueType metric) const;

    /**
     * @brief creates a MetricGroup out of the MonitoringPlan;
     * @return
     */
    [[nodiscard]] MetricGroupPtr createMetricGroup(const MetricCatalogPtr& catalog) const;

    /**
     * @brief
     * @param schema
     * @param buf
     * @return
     */
    GroupedMetricValues fromBuffer(const std::shared_ptr<Schema>& schema, Runtime::TupleBuffer& buf) const;

    /**
     * @brief Returns the schema of the class.
     * @return the schema
     */
    SchemaPtr createSchema() const;

    /**
     * @brief Creates a serializable monitoring plan according to the Protobuf definition.
     * @return the serializable monitoring plan
     */
    [[nodiscard]] SerializableMonitoringPlan serialize() const;

    [[nodiscard]] std::string toString() const;

    friend std::ostream& operator<<(std::ostream&, const MonitoringPlan&);

    static const std::string CPU_METRICS_DESC;
    static const std::string CPU_VALUES_DESC;
    static const std::string NETWORK_METRICS_DESC;
    static const std::string NETWORK_VALUES_DESC;
    static const std::string MEMORY_METRICS_DESC;
    static const std::string DISK_METRICS_DESC;

  private:
    explicit MonitoringPlan(const std::vector<MetricValueType>& metrics);
    explicit MonitoringPlan(const SerializableMonitoringPlan& plan);

    //the metrics for monitoring
    bool cpuMetrics;
    bool networkMetrics;
    bool memoryMetrics;
    bool diskMetrics;
};

using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;

}// namespace NES

#endif// NES_INCLUDE_MONITORING_METRICS_MONITORING_PLAN_HPP_
