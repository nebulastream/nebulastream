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
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Coordinator/LogicalSourceType.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Storage/LatestEntriesMetricStore.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryState.hpp>
#include <nlohmann/json.hpp>
#include <regex>
#include <utility>

namespace NES::Monitoring {
MonitoringManager::MonitoringManager(TopologyPtr topology,
                                     RequestHandlerServicePtr requestHandlerService,
                                     Catalogs::Query::QueryCatalogPtr queryCatalog)
    : MonitoringManager(std::move(topology), std::move(requestHandlerService), std::move(queryCatalog), true) {}

MonitoringManager::MonitoringManager(TopologyPtr topology,
                                     RequestHandlerServicePtr requestHandlerService,
                                     Catalogs::Query::QueryCatalogPtr queryCatalog,
                                     bool enableMonitoring)
    : MonitoringManager(topology,
                        requestHandlerService,
                        queryCatalog,
                        std::make_shared<LatestEntriesMetricStore>(),
                        enableMonitoring) {}

MonitoringManager::MonitoringManager(TopologyPtr topology,
                                     RequestHandlerServicePtr requestHandlerService,
                                     Catalogs::Query::QueryCatalogPtr queryCatalog,
                                     MetricStorePtr metricStore,
                                     bool enableMonitoring)
    : metricStore(metricStore), workerClient(WorkerRPCClient::create()), topology(topology), enableMonitoring(enableMonitoring),
      monitoringCollectors(MonitoringPlan::defaultCollectors()) {
    this->requestHandlerService = requestHandlerService;
    this->queryCatalog = queryCatalog;
    NES_DEBUG("MonitoringManager: Init with monitoring= {} , storage= {} ",
              enableMonitoring,
              std::string(magic_enum::enum_name(metricStore->getType())));
}

MonitoringManager::~MonitoringManager() {
    NES_DEBUG("MonitoringManager: Shutting down");
    workerClient.reset();
    topology.reset();
}

bool MonitoringManager::registerRemoteMonitoringPlans(const std::vector<WorkerId>& nodeIds, MonitoringPlanPtr monitoringPlan) {
    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Register plan failed. Monitoring is disabled.");
        return false;
    }
    if (!monitoringPlan) {
        NES_ERROR("MonitoringManager: Register monitoring plan failed, no plan is provided.");
        return false;
    }
    if (nodeIds.empty()) {
        NES_ERROR("MonitoringManager: Register monitoring plan failed, no nodes are provided.");
        return false;
    }

    for (auto nodeId : nodeIds) {
        NES_DEBUG("MonitoringManager: Registering monitoring plan for worker id= {}", nodeId);
        TopologyNodePtr node = topology->getCopyOfTopologyNodeWithId(nodeId);

        if (node) {
            auto nodeIp = node->getIpAddress();
            auto nodeGrpcPort = node->getGrpcPort();
            std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);

            auto success = workerClient->registerMonitoringPlan(destAddress, monitoringPlan);

            if (success) {
                NES_DEBUG("MonitoringManager: Node with ID {} registered successfully.", nodeId);
                monitoringPlanMap[nodeId] = monitoringPlan;
            } else {
                NES_ERROR("MonitoringManager: Node with ID {} failed to register plan over GRPC.", nodeId);
                return false;
            }
        } else {
            NES_ERROR("MonitoringManager: Node with ID {} does not exit.", nodeId);
            return false;
        }
    }
    return true;
}

nlohmann::json MonitoringManager::requestRemoteMonitoringData(WorkerId nodeId) {
    nlohmann::json metricsJson;
    if (!enableMonitoring) {
        NES_ERROR(
            "MonitoringManager: Requesting monitoring data for node {} failed. Monitoring is disabled, returning empty object",
            nodeId);
        return metricsJson;
    }

    NES_DEBUG("MonitoringManager: Requesting metrics for node id={}", nodeId);
    auto plan = getMonitoringPlan(nodeId);

    //getMonitoringPlan(..) checks if node exists, so no further check necessary
    TopologyNodePtr node = topology->getCopyOfTopologyNodeWithId(nodeId);
    auto nodeIp = node->getIpAddress();
    auto nodeGrpcPort = node->getGrpcPort();
    std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);
    auto metricsAsJsonString = workerClient->requestMonitoringData(destAddress);

    if (!metricsAsJsonString.empty()) {
        NES_DEBUG("MonitoringManager: Received monitoring data {}", metricsAsJsonString);
        //convert string to json object
        metricsJson = metricsJson.parse(metricsAsJsonString);
        return metricsJson;
    }
    NES_THROW_RUNTIME_ERROR("MonitoringManager: Error receiving monitoring metrics for node with id " + node->getId().toString());
}

StoredNodeMetricsPtr MonitoringManager::getMonitoringDataFromMetricStore(WorkerId node) {
    return metricStore->getAllMetrics(node);
}

void MonitoringManager::addMonitoringData(WorkerId nodeId, MetricPtr metrics) {
    NES_TRACE("MonitoringManager: Adding metrics of type {} for node {}",
              std::string(magic_enum::enum_name(metrics->getMetricType())),
              nodeId);
    metricStore->addMetrics(nodeId, metrics);
}

void MonitoringManager::removeMonitoringNode(WorkerId nodeId) {
    NES_DEBUG("MonitoringManager: Removing node and metrics for node {}", nodeId);
    monitoringPlanMap.erase(nodeId);
    metricStore->removeMetrics(nodeId);
}

MonitoringPlanPtr MonitoringManager::getMonitoringPlan(WorkerId nodeId) {
    if (monitoringPlanMap.find(nodeId) == monitoringPlanMap.end()) {
        TopologyNodePtr node = topology->getCopyOfTopologyNodeWithId(nodeId);
        if (node) {
            NES_DEBUG("MonitoringManager: No registered plan found. Returning default plan for node {}", nodeId);
            return MonitoringPlan::defaultPlan();
        }
        NES_THROW_RUNTIME_ERROR("MonitoringManager: Retrieving metrics for " + nodeId.toString()
                                + " failed. Node does not exist in topology.");
    } else {
        return monitoringPlanMap[nodeId];
    }
}

MetricStorePtr MonitoringManager::getMetricStore() { return metricStore; }

bool MonitoringManager::registerLogicalMonitoringStreams(const NES::Configurations::CoordinatorConfigurationPtr config) {
    if (enableMonitoring) {
        for (auto collectorType : monitoringCollectors) {
            auto metricSchema = MetricUtils::getSchemaFromCollectorType(collectorType);
            // auto generate the specifics
            MetricType metricType = MetricUtils::createMetricFromCollectorType(collectorType)->getMetricType();
            std::string logicalSourceName = std::string(magic_enum::enum_name(metricType));
            logicalMonitoringSources.insert(logicalSourceName);
            NES_INFO("MonitoringManager: Creating logical source {}", logicalSourceName);
            config->logicalSourceTypes.add(LogicalSourceType::create(logicalSourceName, metricSchema));
        }
        return true;
    }
    NES_WARNING("MonitoringManager: Monitoring is disabled, registering of logical monitoring streams not possible.");
    return false;
}

QueryId MonitoringManager::startOrRedeployMonitoringQuery(std::string monitoringStream, bool sync) {
    QueryId queryId = INVALID_QUERY_ID;

    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Deploying queries failed. Monitoring is disabled.");
    }

    bool success;
    if (deployedMonitoringQueries.contains(monitoringStream)) {
        success = stopRunningMonitoringQuery(monitoringStream, sync);
    } else {
        success = true;
    }

    // params for iteration
    if (success) {
        MetricType metricType = magic_enum::enum_cast<MetricType>(monitoringStream).value();
        std::string metricCollectorStr =
            std::string(magic_enum::enum_name(MetricUtils::createCollectorTypeFromMetricType(metricType)));
        std::string query =
            R"(Query::from("%STREAM%").sink(MonitoringSinkDescriptor::create(Monitoring::MetricCollectorType::%COLLECTOR%));)";
        query = std::regex_replace(query, std::regex("%STREAM%"), monitoringStream);
        query = std::regex_replace(query, std::regex("%COLLECTOR%"), metricCollectorStr);

        // create new monitoring query
        NES_INFO("MonitoringManager: Creating query for {}", monitoringStream);
        queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
        if ((sync && waitForQueryToStart(queryId, std::chrono::seconds(60))) || (!sync)) {
            NES_INFO("MonitoringManager: Successfully started query {}::{}", queryId, monitoringStream);
            deployedMonitoringQueries.insert({monitoringStream, queryId});
        } else {
            NES_ERROR("MonitoringManager: Query {} :: {} failed to start in time.", queryId, monitoringStream);
        }
    } else {
        NES_ERROR(
            "MonitoringManager: Failed to deploy monitoring query. Queries are still running and could not be stopped for {}",
            monitoringStream);
    }
    return queryId;
}

std::unordered_map<std::string, QueryId> MonitoringManager::startOrRedeployMonitoringQueries(bool sync) {
    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Deploying queries failed. Monitoring is disabled.");
        return deployedMonitoringQueries;
    }

    // params for iteration
    for (auto monitoringStream : logicalMonitoringSources) {
        bool success = stopRunningMonitoringQuery(monitoringStream, sync);

        if (success) {
            startOrRedeployMonitoringQuery(monitoringStream, sync);
        }
    }

    return deployedMonitoringQueries;
}

bool MonitoringManager::stopRunningMonitoringQuery(std::string streamName, bool sync) {
    bool success = true;

    // params for iteration
    if (deployedMonitoringQueries.contains(streamName)) {
        auto metricType = streamName;
        auto queryId = deployedMonitoringQueries.at(streamName);

        NES_INFO("MonitoringManager: Stopping query {} for {}", queryId, metricType);
        if (requestHandlerService->validateAndQueueStopQueryRequest(queryId)) {
            if ((sync && checkStoppedOrTimeout(queryId, std::chrono::seconds(60))) || (!sync)) {
                NES_INFO("MonitoringManager: Query {}::{} terminated", queryId, metricType);
            } else {
                NES_ERROR("MonitoringManager: Failed to stop query {}::{}", queryId, metricType);
                success = false;
            }
        } else {
            NES_ERROR("MonitoringManager: Failed to validate query {}::{}", queryId, metricType);
            success = false;
        }
    }
    if (success) {
        deployedMonitoringQueries.erase(streamName);
        NES_INFO("MonitoringManager: Monitoring query stopped successfully {}", streamName);
    }
    return success;
}

bool MonitoringManager::stopRunningMonitoringQueries(bool sync) {
    bool success = true;
    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Deploying queries failed. Monitoring is disabled.");
    }

    // params for iteration
    for (auto monitoringStream : logicalMonitoringSources) {
        if (!stopRunningMonitoringQuery(monitoringStream, sync)) {
            success = false;
        }
    }

    return success;
}

bool MonitoringManager::checkStoppedOrTimeout(QueryId queryId, std::chrono::seconds timeout) {
    auto timeoutInSec = std::chrono::seconds(timeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkStoppedOrTimeout: check query status for {}", queryId);
        QueryState queryState = queryCatalog->getQueryState(queryId);
        if (queryState == QueryState::STOPPED) {
            NES_TRACE("checkStoppedOrTimeout: status for {} reached stopped", queryId);
            return true;
        }
        NES_DEBUG("checkStoppedOrTimeout: status not reached for {} as status is={}", queryId, magic_enum::enum_name(queryState));
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
    NES_TRACE("checkStoppedOrTimeout: expected status not reached within set timeout");
    return false;
}

bool MonitoringManager::waitForQueryToStart(QueryId queryId, std::chrono::seconds timeout) {
    NES_INFO("MonitoringManager: wait till the query {} gets into Running status.", queryId);
    auto start_timestamp = std::chrono::system_clock::now();

    while (std::chrono::system_clock::now() < start_timestamp + timeout) {
        auto queryState = queryCatalog->getQueryState(queryId);
        NES_TRACE("MonitoringManager: Query {} is now in status {}", queryId, magic_enum::enum_name(queryState));
        switch (queryState) {
            case QueryState::RUNNING: {
                NES_DEBUG("MonitoringManager: Query is now running {}", queryId);
                return true;
            }
            default: {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
    NES_ERROR("MonitoringManager: Starting query {} has timed out.", queryId);
    return false;
}

bool MonitoringManager::isMonitoringStream(std::string streamName) const { return logicalMonitoringSources.contains(streamName); }

const std::unordered_map<std::string, QueryId>& MonitoringManager::getDeployedMonitoringQueries() const {
    return deployedMonitoringQueries;
}

}// namespace NES::Monitoring
