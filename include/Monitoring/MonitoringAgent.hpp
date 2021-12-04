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

#ifndef NES_INCLUDE_MONITORING_MONITORING_AGENT_HPP_
#define NES_INCLUDE_MONITORING_MONITORING_AGENT_HPP_

#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <memory>
#include <unordered_map>
#include <optional>

namespace NES {

class MonitoringPlan;
class TupleBuffer;
class MetricCatalog;
class Schema;
class MonitoringAgent;
class StaticNesMetrics;
class RuntimeNesMetrics;

using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;
using MetricCatalogPtr = std::shared_ptr<MetricCatalog>;
using SchemaPtr = std::shared_ptr<Schema>;
using MonitoringAgentPtr = std::shared_ptr<MonitoringAgent>;
using StaticNesMetricsPtr = std::shared_ptr<StaticNesMetrics>;
using RuntimeNesMetricsPtr = std::shared_ptr<RuntimeNesMetrics>;

/**
 * @brief The MonitoringAgent which is responsible for collecting metrics on a local level.
 */
class MonitoringAgent {
  public:
    MonitoringAgent();
    MonitoringAgent(bool enabled);
    MonitoringAgent(const MonitoringPlanPtr& monitoringPlan, MetricCatalogPtr catalog, bool enabled);

    static MonitoringAgentPtr create();
    static MonitoringAgentPtr create(bool enabled);
    static MonitoringAgentPtr create(const MonitoringPlanPtr& monitoringPlan, const MetricCatalogPtr& catalog, bool enabled);

    /**
     * @brief Register a monitoring plan at the local worker. The plan is indicating which metrics to collect.
     * @param monitoringPlan
     * @return the schema of the monitoring plan
     */
    SchemaPtr registerMonitoringPlan(const MonitoringPlanPtr& monitoringPlan);

    /**
     * @brief Collect the metrics and store them in to the given tuple buffer. The collected metrics depend on the monitoring plan.
     * If no plan is provided, then all metrics will be returned as default.
     * @param tupleBuffer
     * @return the schema of the monitoring plan
     */
    bool getMetricsFromPlan(Runtime::TupleBuffer& tupleBuffer);

    /**
     * @brief Return the schema based on the monitoring plan. If no schema is provided then the default schema is return, which
     * contains all metrics.
     * @return the schema
     */
    SchemaPtr getSchema();

    /**
     * @brief Returns the static metrics used for NES.
     * @return The metric object.
     */
    [[nodiscard]] std::optional<StaticNesMetricsPtr> getStaticNesMetrics() const;

    /**
     * @brief Returns the static metrics used for NES.
     * @return The metric object.
     */
    [[nodiscard]] std::optional<RuntimeNesMetricsPtr> getRuntimeNesMetrics() const;

    /**
     * @brief Enables collecting of metrics
     * @param If enabled then true, else false
     */
    void setEnableMonitoring(bool enable);

    /**
     * @brief Checks if monitoring is enabled
     * @return If enabled then true, else false
     */
    [[nodiscard]] bool isEnabled() const;

  private:
    MonitoringPlanPtr monitoringPlan;
    MetricCatalogPtr catalog;
    SchemaPtr schema;
    bool enabled;
};

}// namespace NES

#endif// NES_INCLUDE_MONITORING_MONITORING_AGENT_HPP_
