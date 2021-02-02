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

#ifndef NES_INCLUDE_MONITORING_METRICS_MONITORINGPLAN_HPP_
#define NES_INCLUDE_MONITORING_METRICS_MONITORINGPLAN_HPP_

#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace NES {

class MetricCatalog;
class MetricGroup;
class GroupedValues;
class SerializableMonitoringPlan;
class MonitoringPlan;
class MetricCatalog;

typedef std::shared_ptr<MetricCatalog> MetricCatalogPtr;
typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;
typedef std::shared_ptr<MetricGroup> MetricGroupPtr;

class MonitoringPlan {
  public:
    static MonitoringPlanPtr create(const std::vector<MetricValueType>& metrics);
    static MonitoringPlanPtr create(const SerializableMonitoringPlan& shippable);

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
    bool hasMetric(MetricValueType metric) const;

    /**
     * @brief creates a MetricGroup out of the MonitoringPlan;
     * @return
     */
    MetricGroupPtr createMetricGroup(MetricCatalogPtr catalog) const;

    /**
     * @brief
     * @param schema
     * @param buf
     * @return
     */
    GroupedValues fromBuffer(std::shared_ptr<Schema> schema, NodeEngine::TupleBuffer& buf);

    /**
     * @brief Creates a serializable monitoring plan according to the Protobuf definition.
     * @return the serializable monitoring plan
     */
    SerializableMonitoringPlan serialize() const;

    std::string toString() const;

    friend std::ostream& operator<<(std::ostream&, const MonitoringPlan&);

  public:
    static const std::string CPU_METRICS_DESC;
    static const std::string CPU_VALUES_DESC;
    static const std::string NETWORK_METRICS_DESC;
    static const std::string NETWORK_VALUES_DESC;
    static const std::string MEMORY_METRICS_DESC;
    static const std::string DISK_METRICS_DESC;

  private:
    MonitoringPlan(const std::vector<MetricValueType>& metrics);
    MonitoringPlan(const SerializableMonitoringPlan& shippable);

    //the metrics for monitoring
    bool cpuMetrics;
    bool networkMetrics;
    bool memoryMetrics;
    bool diskMetrics;
};

typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_MONITORINGPLAN_HPP_
