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

#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MonitoringSourceType.hpp>
#include <Components/NesWorker.hpp>
#include <Monitoring/MetricCollectors/MetricCollector.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/MetricType.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <Monitoring/MonitoringCatalog.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/ResourcesReader/AbstractSystemResourcesReader.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {
using namespace Configurations;

MonitoringAgent::MonitoringAgent() : MonitoringAgent(true) {}

MonitoringAgent::MonitoringAgent(bool enabled)
    : MonitoringAgent(MonitoringPlan::defaultPlan(), MonitoringCatalog::defaultCatalog(), enabled) {}

MonitoringAgent::MonitoringAgent(MonitoringPlanPtr monitoringPlan, MonitoringCatalogPtr catalog, bool enabled)
    : monitoringPlan(monitoringPlan), catalog(catalog), enabled(enabled) {
    NES_DEBUG("MonitoringAgent: Init with monitoring plan {} and enabled={}", monitoringPlan->toString(), enabled);
}

MonitoringAgentPtr MonitoringAgent::create() { return std::make_shared<MonitoringAgent>(); }

MonitoringAgentPtr MonitoringAgent::create(bool enabled) { return std::make_shared<MonitoringAgent>(enabled); }

MonitoringAgentPtr MonitoringAgent::create(MonitoringPlanPtr monitoringPlan, MonitoringCatalogPtr catalog, bool enabled) {
    return std::make_shared<MonitoringAgent>(monitoringPlan, catalog, enabled);
}

const std::vector<MetricPtr> MonitoringAgent::getMetricsFromPlan() const {
    std::vector<MetricPtr> output;
    if (enabled) {
        NES_DEBUG("MonitoringAgent: Monitoring enabled, reading metrics for getMetricsFromPlan().");
        for (auto type : monitoringPlan->getMetricTypes()) {
            auto collector = catalog->getMetricCollector(type);
            collector->setNodeId(nodeId);
            MetricPtr metric = collector->readMetric();
            output.emplace_back(metric);
        }
    } else {
        NES_WARNING("MonitoringAgent: Monitoring disabled, getMetricsFromPlan() returns empty vector.");
    }
    return output;
}

bool MonitoringAgent::isEnabled() const { return enabled; }

MonitoringPlanPtr MonitoringAgent::getMonitoringPlan() const { return monitoringPlan; }

void MonitoringAgent::setMonitoringPlan(const MonitoringPlanPtr monitoringPlan) { this->monitoringPlan = monitoringPlan; }

nlohmann::json MonitoringAgent::getMetricsAsJson() {
    nlohmann::json metricsJson{};
    if (enabled) {
        for (auto type : monitoringPlan->getMetricTypes()) {
            NES_INFO("MonitoringAgent: Collecting metrics of type {}", std::string(magic_enum::enum_name(type)));
            auto collector = catalog->getMetricCollector(type);
            collector->setNodeId(nodeId);
            auto metric = collector->readMetric();
            metricsJson[std::string(magic_enum::enum_name(metric->getMetricType()))] = asJson(metric);
        }
    }
    NES_INFO("MonitoringAgent: Metrics collected {}", metricsJson.dump());

    return metricsJson;
}

RegistrationMetrics MonitoringAgent::getRegistrationMetrics() {
    if (enabled) {
        return SystemResourcesReaderFactory::getSystemResourcesReader()->readRegistrationMetrics();
    }
    NES_WARNING("MonitoringAgent: Metrics disabled. Return empty metric object for registration.");
    return RegistrationMetrics{};
}

bool MonitoringAgent::addMonitoringStreams(const Configurations::WorkerConfigurationPtr workerConfig) {
    if (enabled) {
        for (auto metricType : monitoringPlan->getMetricTypes()) {
            // auto generate the specifics
            MonitoringSourceTypePtr sourceType =
                MonitoringSourceType::create(MetricUtils::createCollectorTypeFromMetricType(metricType),
                                             std::chrono::milliseconds(workerConfig->monitoringWaitTime.getValue()));
            std::string metricTypeString = std::string(magic_enum::enum_name(metricType));

            NES_INFO("MonitoringAgent: Adding physical source to config {} _ph with wait time {}",
                     metricTypeString,
                     workerConfig->monitoringWaitTime.getValue());
            auto source = PhysicalSource::create(metricTypeString, metricTypeString + "_ph", sourceType);
            workerConfig->physicalSources.add(source);
        }
        return true;
    }
    NES_WARNING("MonitoringAgent: Monitoring is disabled, registering of physical monitoring streams not possible.");
    return false;
}

void MonitoringAgent::setNodeId(TopologyNodeId nodeId) { this->nodeId = nodeId; }

}// namespace NES::Monitoring