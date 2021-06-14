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

#ifndef NES_INCLUDE_MONITORING_MONITORINGAGENT_HPP_
#define NES_INCLUDE_MONITORING_MONITORINGAGENT_HPP_

#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <memory>
#include <unordered_map>

namespace NES {

class MonitoringPlan;
class TupleBuffer;
class MetricCatalog;
class Schema;
class MonitoringAgent;

typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;
typedef std::shared_ptr<MetricCatalog> MetricCatalogPtr;
typedef std::shared_ptr<Schema> SchemaPtr;
typedef std::shared_ptr<MonitoringAgent> MonitoringAgentPtr;

/**
 * @brief The MonitoringAgent which is responsible for collecting metrics on a local level.
 */
class MonitoringAgent {
  public:
    MonitoringAgent();
    MonitoringAgent(MonitoringPlanPtr monitoringPlan, MetricCatalogPtr catalog);
    static MonitoringAgentPtr create();
    static MonitoringAgentPtr create(MonitoringPlanPtr monitoringPlan, MetricCatalogPtr catalog);

    /**
     * @brief Register a monitoring plan at the local worker. The plan is indicating which metrics to collect.
     * @param monitoringPlan
     * @return the schema of the monitoring plan
     */
    SchemaPtr registerMonitoringPlan(MonitoringPlanPtr monitoringPlan);

    /**
     * @brief Collect the metrics and store them in to the given tuple buffer. The collected metrics depend on the monitoring plan.
     * If no plan is provided, then all metrics will be returned as default.
     * @param tupleBuffer
     * @return the schema of the monitoring plan
     */
    bool getMetrics(NodeEngine::TupleBuffer& tupleBuffer);

    /**
     * @brief Return the schema based on the monitoring plan. If no schema is provided then the default schema is return, which
     * contains all metrics.
     * @return the schema
     */
    SchemaPtr getSchema();

  private:
    MonitoringPlanPtr monitoringPlan;
    MetricCatalogPtr catalog;
    SchemaPtr schema;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_MONITORINGAGENT_HPP_
