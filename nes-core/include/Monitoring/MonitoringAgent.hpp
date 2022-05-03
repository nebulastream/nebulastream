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

#ifndef NES_INCLUDE_MONITORING_MONITORING_AGENT_HPP_
#define NES_INCLUDE_MONITORING_MONITORING_AGENT_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <memory>
#include <unordered_map>

namespace NES {

class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;
class RegistrationMetrics;

/**
* @brief The MonitoringAgent which is responsible for collecting metrics on a local level.
*/
class MonitoringAgent {
  public:
    MonitoringAgent();
    explicit MonitoringAgent(bool enabled);
    MonitoringAgent(MonitoringPlanPtr monitoringPlan, MonitoringCatalogPtr catalog, bool enabled);

    static MonitoringAgentPtr create();
    static MonitoringAgentPtr create(bool enabled);
    static MonitoringAgentPtr create(MonitoringPlanPtr monitoringPlan, MonitoringCatalogPtr catalog, bool enabled);

    /**
     * @brief Collect the metrics and store them in to the given output. The collected metrics depend on the monitoring plan.
     * If monitoring is disabled an empty vector will be returned.
     * @return the metrics of the monitoring plan
    */
    std::vector<MetricPtr> getMetricsFromPlan();

    /**
     * @brief Returns all metrics defined by the plan as a unified JSON.
     * @return Metrics as JSON.
    */
    web::json::value getMetricsAsJson();

    /**
     * @brief Checks if monitoring is enabled
     * @return If enabled then true, else false
    */
    [[nodiscard]] bool isEnabled() const;

    /**
     * @brief Create monitoring sources for the provided MonitoringPlan and handle them as data sources within the NES engine.
     * If monitoring is disabled this method is not doing anything.
     * @param nesWorker
    */
    void startContinuousMonitoring(NesWorkerPtr nesWorker);

    /**
     * @brief Getter for the MonitoringPlan
     * @return Ptr to the MonitoringPlan
    */
    [[nodiscard]] MonitoringPlanPtr getMonitoringPlan() const;

    /**
     * @brief set the monitoring plan
     * @param monitoringPlan
    */
    void setMonitoringPlan(MonitoringPlanPtr monitoringPlan);

    /**
     * @brief Return the metrics required for node registration.
     * @return the metrics
     */
    RegistrationMetrics getRegistrationMetrics();

  private:
    MonitoringPlanPtr monitoringPlan;
    MonitoringCatalogPtr catalog;
    bool enabled;
};

}// namespace NES

#endif// NES_INCLUDE_MONITORING_MONITORING_AGENT_HPP_
