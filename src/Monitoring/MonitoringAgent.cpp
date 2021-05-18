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

#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <Util/Logger.hpp>

namespace NES {

MonitoringAgent::MonitoringAgent() : catalog(MetricCatalog::NesMetrics()) {
    NES_DEBUG("MonitoringAgent: Init with default monitoring plan");
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    monitoringPlan = MonitoringPlan::create(metrics);
    schema = monitoringPlan->createSchema();
}

MonitoringAgent::MonitoringAgent(MonitoringPlanPtr monitoringPlan, MetricCatalogPtr catalog)
    : monitoringPlan(monitoringPlan), catalog(catalog), schema(monitoringPlan->createSchema()) {
    NES_DEBUG("MonitoringAgent: Init with monitoring plan " + monitoringPlan->toString());
}

SchemaPtr MonitoringAgent::registerMonitoringPlan(MonitoringPlanPtr monitoringPlan) {
    this->monitoringPlan = monitoringPlan;
    schema = monitoringPlan->createSchema();
    return schema;
}

SchemaPtr MonitoringAgent::getMetrics(TupleBuffer& tupleBuffer) {
    MetricGroupPtr metricGroup = monitoringPlan->createMetricGroup(catalog);
    metricGroup->getSample(tupleBuffer);
    return schema;
}

SchemaPtr MonitoringAgent::getSchema() {
    return schema;
}

}// namespace NES