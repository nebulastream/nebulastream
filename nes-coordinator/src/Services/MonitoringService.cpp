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
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Services/MonitoringService.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <nlohmann/json.hpp>
#include <utility>

namespace NES {
MonitoringService::MonitoringService(TopologyPtr topology,
                                     RequestHandlerServicePtr requestHandlerService,
                                     QueryCatalogServicePtr catalogService)
    : MonitoringService(topology, requestHandlerService, catalogService, true) {}

MonitoringService::MonitoringService(TopologyPtr topology,
                                     RequestHandlerServicePtr requestHandlerService,
                                     QueryCatalogServicePtr catalogService,
                                     bool enable)
    : topology(topology), enableMonitoring(enable) {
    NES_DEBUG("MonitoringService: Initializing with monitoring= {}", enable);
    monitoringManager =
        std::make_shared<Monitoring::MonitoringManager>(topology, requestHandlerService, catalogService, enableMonitoring);
}

nlohmann::json MonitoringService::registerMonitoringPlanToAllNodes(Monitoring::MonitoringPlanPtr monitoringPlan) {
    nlohmann::json metricsJson;
    auto nodeIds = topology->getAllRegisteredNodeIds();
    auto success = monitoringManager->registerRemoteMonitoringPlans(nodeIds, std::move(monitoringPlan));
    metricsJson["success"] = success;
    return metricsJson;
}

nlohmann::json MonitoringService::requestMonitoringDataAsJson(uint64_t nodeId) {
    NES_DEBUG("MonitoringService: Requesting monitoring data from worker id= {}", std::to_string(nodeId));
    return monitoringManager->requestRemoteMonitoringData(nodeId);
}

nlohmann::json MonitoringService::requestMonitoringDataFromAllNodesAsJson() {
    nlohmann::json metricsJson;
    auto nodeIds = topology->getAllRegisteredNodeIds();
    for (const auto& nodeId : nodeIds) {
        NES_INFO("MonitoringService: Requesting metrics for node {}", std::to_string(nodeId));
        metricsJson[std::to_string(nodeId)] = requestMonitoringDataAsJson(nodeId);
        Monitoring::StoredNodeMetricsPtr tMetrics = monitoringManager->getMonitoringDataFromMetricStore(nodeId);
        metricsJson[std::to_string(nodeId)][toString(Monitoring::MetricType::RegistrationMetric)] =
            Monitoring::MetricUtils::toJson(tMetrics)[toString(Monitoring::MetricType::RegistrationMetric)][0]["value"];
    }
    return metricsJson;
}

nlohmann::json MonitoringService::requestNewestMonitoringDataFromMetricStoreAsJson() {
    nlohmann::json metricsJson;
    auto nodeIds = topology->getAllRegisteredNodeIds();
    for (const auto& nodeId : nodeIds) {
        NES_INFO("MonitoringService: Requesting metrics for node {}", std::to_string(nodeId));
        Monitoring::StoredNodeMetricsPtr parsedValues = monitoringManager->getMonitoringDataFromMetricStore(nodeId);
        metricsJson[std::to_string(nodeId)] = Monitoring::MetricUtils::toJson(parsedValues);
    }
    NES_INFO("MonitoringService: MetricTypes from coordinator received\n{}", metricsJson.dump());
    return metricsJson;
}

const Monitoring::MonitoringManagerPtr MonitoringService::getMonitoringManager() const { return monitoringManager; }

bool MonitoringService::isMonitoringEnabled() const { return enableMonitoring; }

nlohmann::json MonitoringService::startMonitoringStreams() {
    nlohmann::json output;
    auto queryIds = monitoringManager->startOrRedeployMonitoringQueries(false);

    nlohmann::json elem{};
    int i = 0;
    for (auto queryIdPair : queryIds) {
        elem["logical_stream"] = queryIdPair.first;
        elem["query_ID"] = queryIdPair.second;
        output[i++] = elem;
    }

    return output;
}

nlohmann::json MonitoringService::stopMonitoringStreams() {
    monitoringManager->stopRunningMonitoringQueries(false);
    return nlohmann::json::boolean_t(true);
}

nlohmann::json MonitoringService::getMonitoringStreams() {
    nlohmann::json output;
    auto queryIds = monitoringManager->getDeployedMonitoringQueries();

    nlohmann::json elem{};
    int i = 0;
    for (auto queryIdPair : queryIds) {
        elem["logical_stream"] = queryIdPair.first;
        elem["query_ID"] = queryIdPair.second;
        output[i++] = elem;
    }

    return output;
}

}// namespace NES