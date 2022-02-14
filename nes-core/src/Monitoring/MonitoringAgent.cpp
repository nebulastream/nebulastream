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

#include <API/Schema.hpp>
#include <Components/NesWorker.hpp>
#include <Monitoring/MetricCollectors/MetricCollector.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <Monitoring/MonitoringCatalog.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <cpprest/json.h>

namespace NES {
using namespace Configurations;

MonitoringAgent::MonitoringAgent() : MonitoringAgent(true) {}

MonitoringAgent::MonitoringAgent(bool enabled)
    : MonitoringAgent(MonitoringPlan::createDefaultPlan(), MonitoringCatalog::defaultCatalog(), enabled) {}

MonitoringAgent::MonitoringAgent(MonitoringPlanPtr monitoringPlan, MonitoringCatalogPtr catalog, bool enabled)
    : monitoringPlan(monitoringPlan), catalog(catalog), enabled(enabled) {
    NES_DEBUG("MonitoringAgent: Init with monitoring plan " + monitoringPlan->toString() + " and enabled=" << enabled);
}

MonitoringAgentPtr MonitoringAgent::create() { return std::make_shared<MonitoringAgent>(); }

MonitoringAgentPtr MonitoringAgent::create(bool enabled) { return std::make_shared<MonitoringAgent>(enabled); }

MonitoringAgentPtr MonitoringAgent::create(MonitoringPlanPtr monitoringPlan, MonitoringCatalogPtr catalog, bool enabled) {
    return std::make_shared<MonitoringAgent>(monitoringPlan, catalog, enabled);
}

void MonitoringAgent::startContinuousMonitoring(NesWorkerPtr) {
    if (enabled) {
        NES_NOT_IMPLEMENTED();
    } else {
        NES_INFO("MonitoringDisabled: ");
    }
}

std::vector<MetricPtr> MonitoringAgent::getMetricsFromPlan() {
    std::vector<MetricPtr> output;
    if (enabled) {
        for (auto type : monitoringPlan->getMetricTypes()) {
            auto collector = catalog->getMetricCollector(type);
            MetricPtr metric = collector->readMetric();
            output.emplace_back(metric);
        }
    }
    NES_WARNING("MonitoringAgent: Monitoring disabled, getMetricsFromPlan() returns empty vector.");
    return output;
}

bool MonitoringAgent::isEnabled() const { return enabled; }

MonitoringPlanPtr MonitoringAgent::getMonitoringPlan() const { return monitoringPlan; }

void MonitoringAgent::setMonitoringPlan(const MonitoringPlanPtr monitoringPlan) { this->monitoringPlan = monitoringPlan; }

web::json::value MonitoringAgent::getMetricsAsJson() {
    web::json::value metricsJson{};
    if (enabled) {
        for (auto type : monitoringPlan->getMetricTypes()) {
            auto collector = catalog->getMetricCollector(type);
            MetricPtr metric = collector->readMetric();
            metricsJson[toString(getMetricType(metric))] = asJson(metric);
        }
    }
    return metricsJson;
}

RegistrationMetrics MonitoringAgent::getRegistrationMetrics() {
    if (enabled) {
        return SystemResourcesReaderFactory::getSystemResourcesReader()->readRegistrationMetrics();
    }
    NES_WARNING("MonitoringAgent: Metrics disabled. Return empty metric object for registration.");
    return RegistrationMetrics{};
}

}// namespace NES