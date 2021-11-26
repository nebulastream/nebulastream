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
#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <Monitoring/MetricValues/RuntimeNesMetrics.hpp>
#include <Monitoring/MetricValues/StaticNesMetrics.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES {

MonitoringAgent::MonitoringAgent() : MonitoringAgent(MonitoringPlan::DefaultPlan(), MetricCatalog::NesMetrics(), true) {}

MonitoringAgent::MonitoringAgent(bool enabled)
    : MonitoringAgent(MonitoringPlan::DefaultPlan(), MetricCatalog::NesMetrics(), enabled) {}

MonitoringAgent::MonitoringAgent(const MonitoringPlanPtr& monitoringPlan, MetricCatalogPtr catalog, bool enabled)
    : monitoringPlan(monitoringPlan), catalog(std::move(catalog)), schema(monitoringPlan->createSchema()), enabled(enabled) {
    NES_DEBUG("MonitoringAgent: Init with monitoring plan " + monitoringPlan->toString() + " and enabled=" << enabled);
}

MonitoringAgentPtr MonitoringAgent::create() { return std::make_shared<MonitoringAgent>(); }

MonitoringAgentPtr MonitoringAgent::create(bool enabled) { return std::make_shared<MonitoringAgent>(enabled); }

MonitoringAgentPtr
MonitoringAgent::create(const MonitoringPlanPtr& monitoringPlan, const MetricCatalogPtr& catalog, bool enabled) {
    return std::make_shared<MonitoringAgent>(monitoringPlan, catalog, enabled);
}

SchemaPtr MonitoringAgent::registerMonitoringPlan(const MonitoringPlanPtr& monitoringPlan) {
    this->monitoringPlan = monitoringPlan;
    schema = monitoringPlan->createSchema();
    return schema;
}

bool MonitoringAgent::getMetricsFromPlan(Runtime::TupleBuffer& tupleBuffer) {
    if (enabled) {
        MetricGroupPtr metricGroup = monitoringPlan->createMetricGroup(catalog);
        metricGroup->getSample(tupleBuffer);
        return true;
    }
    NES_WARNING("MonitoringAgent: Monitoring disabled, getMetricsFromPlan() failed.");
    return false;
}

SchemaPtr MonitoringAgent::getSchema() { return schema; }

std::optional<StaticNesMetricsPtr> MonitoringAgent::getStaticNesMetrics() const {
    if (enabled) {
        auto staticStats = MetricUtils::staticNesStats();
        auto measuredVal = std::make_shared<StaticNesMetrics>(staticStats.measure());
        return measuredVal;
    }
    NES_WARNING("MonitoringAgent: Monitoring disabled, getStaticNesMetrics() returns empty object.");
    return std::nullopt;
}

std::optional<RuntimeNesMetricsPtr> MonitoringAgent::getRuntimeNesMetrics() const {
    if (enabled) {
        auto runtimeStats = MetricUtils::runtimeNesStats();
        auto measuredVal = std::make_shared<RuntimeNesMetrics>(runtimeStats.measure());
        return measuredVal;
    }
    NES_WARNING("MonitoringAgent: Monitoring disabled, getRuntimeNesMetrics() returns empty object.");
    return std::nullopt;
}

bool MonitoringAgent::isEnabled() const { return enabled; }

void MonitoringAgent::setEnableMonitoring(bool enable) {
    NES_INFO("MonitoringAgent: Setting enable monitoring=" << enable);
    enable = enabled;
}

}// namespace NES