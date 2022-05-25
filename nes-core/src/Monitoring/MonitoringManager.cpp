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

#include <Monitoring/MonitoringManager.hpp>
#include <Util/Logger/Logger.hpp>

#include <Components/NesCoordinator.hpp>

#include <API/Schema.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Storage/LatestEntriesMetricStore.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/NodeEngine.hpp>

#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>

#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/QueryStatus.hpp>
#include <Util/UtilityFunctions.hpp>
#include <regex>
#include <utility>

namespace NES {
MonitoringManager::MonitoringManager(WorkerRPCClientPtr workerClient, TopologyPtr topology)
    : MonitoringManager(workerClient, topology, true) {}

MonitoringManager::MonitoringManager(WorkerRPCClientPtr workerClient, TopologyPtr topology, bool enableMonitoring)
    : MonitoringManager(workerClient, topology, std::make_shared<LatestEntriesMetricStore>(), enableMonitoring) {}

MonitoringManager::MonitoringManager(WorkerRPCClientPtr workerClient,
                                     TopologyPtr topology,
                                     MetricStorePtr metricStore,
                                     bool enableMonitoring)
    : metricStore(metricStore), workerClient(workerClient), topology(topology), enableMonitoring(enableMonitoring),
      monitoringCollectors(MonitoringPlan::defaultCollectors()) {
    NES_DEBUG("MonitoringManager: Init with monitoring=" << enableMonitoring << ", storage=" << toString(metricStore->getType()));
}

MonitoringManager::~MonitoringManager() {
    NES_DEBUG("MonitoringManager: Shutting down");
    workerClient.reset();
    topology.reset();
}

bool MonitoringManager::registerRemoteMonitoringPlans(const std::vector<uint64_t>& nodeIds, MonitoringPlanPtr monitoringPlan) {
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
        NES_DEBUG("MonitoringManager: Registering monitoring plan for worker id= " + std::to_string(nodeId));
        TopologyNodePtr node = topology->findNodeWithId(nodeId);

        if (node) {
            auto nodeIp = node->getIpAddress();
            auto nodeGrpcPort = node->getGrpcPort();
            std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);

            auto success = workerClient->registerMonitoringPlan(destAddress, monitoringPlan);

            if (success) {
                NES_DEBUG("MonitoringManager: Node with ID " + std::to_string(nodeId) + " registered successfully.");
                monitoringPlanMap[nodeId] = monitoringPlan;
            } else {
                NES_ERROR("MonitoringManager: Node with ID " + std::to_string(nodeId) + " failed to register plan over GRPC.");
                return false;
            }
        } else {
            NES_ERROR("MonitoringManager: Node with ID " + std::to_string(nodeId) + " does not exit.");
            return false;
        }
    }
    return true;
}

web::json::value MonitoringManager::requestRemoteMonitoringData(uint64_t nodeId) {
    web::json::value metricsJson;
    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Requesting monitoring data for node " << nodeId
                                                                            << " failed. Monitoring is disabled, "
                                                                               "returning empty object");
        return metricsJson;
    }

    NES_DEBUG("MonitoringManager: Requesting metrics for node id=" + std::to_string(nodeId));
    auto plan = getMonitoringPlan(nodeId);

    //getMonitoringPlan(..) checks if node exists, so no further check necessary
    TopologyNodePtr node = topology->findNodeWithId(nodeId);
    auto nodeIp = node->getIpAddress();
    auto nodeGrpcPort = node->getGrpcPort();
    std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);
    auto metricsAsJsonString = workerClient->requestMonitoringData(destAddress);

    if (!metricsAsJsonString.empty()) {
        NES_DEBUG("MonitoringManager: Received monitoring data " + metricsAsJsonString);
        //convert string to json object
        metricsJson = metricsJson.parse(metricsAsJsonString);
        return metricsJson;
    }
    NES_THROW_RUNTIME_ERROR("MonitoringManager: Error receiving monitoring metrics for node with id "
                            + std::to_string(node->getId()));
}

StoredNodeMetricsPtr MonitoringManager::getMonitoringDataFromMetricStore(uint64_t node) {
    return metricStore->getAllMetrics(node);
}

void MonitoringManager::addMonitoringData(uint64_t nodeId, MetricPtr metrics) {
    NES_DEBUG("MonitoringManager: Adding metrics for node " << nodeId);
    metricStore->addMetrics(nodeId, metrics);
}

void MonitoringManager::removeMonitoringNode(uint64_t nodeId) {
    NES_DEBUG("MonitoringManager: Removing node and metrics for node " << nodeId);
    monitoringPlanMap.erase(nodeId);
    metricStore->removeMetrics(nodeId);
}

MonitoringPlanPtr MonitoringManager::getMonitoringPlan(uint64_t nodeId) {
    if (monitoringPlanMap.find(nodeId) == monitoringPlanMap.end()) {
        TopologyNodePtr node = topology->findNodeWithId(nodeId);
        if (node) {
            NES_DEBUG("MonitoringManager: No registered plan found. Returning default plan for node " + std::to_string(nodeId));
            return MonitoringPlan::defaultPlan();
        }
        NES_THROW_RUNTIME_ERROR("MonitoringManager: Retrieving metrics for " + std::to_string(nodeId)
                                + " failed. Node does not exist in topology.");
    } else {
        return monitoringPlanMap[nodeId];
    }
}

MetricStorePtr MonitoringManager::getMetricStore() { return metricStore; }

bool MonitoringManager::registerLogicalMonitoringStreams(const Configurations::CoordinatorConfigurationPtr config) {
    if (enableMonitoring) {
        for (auto collectorType : monitoringCollectors) {
            auto metricSchema = MetricUtils::getSchemaFromCollectorType(collectorType);
            // auto generate the specifics
            MetricType metricType = MetricUtils::createMetricFromCollectorType(collectorType)->getMetricType();
            std::string logicalSourceName = NES::toString(metricType);
            logicalMonitoringSources.insert(logicalSourceName);
            NES_INFO("MonitoringManager: Creating logical source " << logicalSourceName);
            config->logicalSources.add(LogicalSource::create(logicalSourceName, metricSchema));
        }
        return true;
    }
    NES_ERROR("MonitoringManager: Monitoring is disabled, registering of logical monitoring streams not possible.");
    return false;
}

std::unordered_map<MetricType, QueryId> MonitoringManager::startOrRedeployMonitoringQueries(NesCoordinatorPtr crd, bool sync) {
    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Deploying queries failed. Monitoring is disabled.");
        return deployedMonitoringQueries;
    }

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr catalogService = crd->getQueryCatalogService();
    std::unordered_map<MetricType, QueryId> newQueries;
    bool success = stopRunningMonitoringQueries(crd, sync);

    // params for iteration
    if (success) {
        for (auto collectorType : monitoringCollectors) {
            MetricType metricType = MetricUtils::createMetricFromCollectorType(collectorType)->getMetricType();
            std::string metricCollectorStr = NES::toString(collectorType);
            std::string metricTypeString = NES::toString(metricType);
            std::string query =
                R"(Query::from("%STREAM%").sink(MonitoringSinkDescriptor::create(MetricCollectorType::%COLLECTOR%));)";
            query = std::regex_replace(query, std::regex("%COLLECTOR%"), metricCollectorStr);
            query = std::regex_replace(query, std::regex("%STREAM%"), metricTypeString);

            // create new monitoring query
            NES_INFO("MonitoringManager: Creating query for " << NES::toString(metricType));
            QueryId queryId =
                queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
            if (sync && waitForQueryToStart(queryId, catalogService, std::chrono::seconds(60))) {
                NES_INFO("MonitoringManager: Successfully started query " << queryId << "::" << NES::toString(metricType));
            } else {
                NES_ERROR("MonitoringManager: Query " << queryId << "::" << NES::toString(metricType)
                                                      << " failed to start in time.");
                continue;
            }
            newQueries.insert({metricType, queryId});
        }
    } else {
        NES_ERROR("MonitoringManager: Failed to deploy monitoring queries. Queries are still running and could not be stopped.");
        return deployedMonitoringQueries;
    }

    deployedMonitoringQueries = std::move(newQueries);
    return deployedMonitoringQueries;
}

bool MonitoringManager::stopRunningMonitoringQueries(NesCoordinatorPtr crd, bool sync) {
    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr catalogService = crd->getQueryCatalogService();
    std::unordered_map<MetricType, QueryId> newQueries;
    bool success = true;

    // params for iteration
    for (auto deployedQuery : deployedMonitoringQueries) {
        auto metricType = deployedQuery.first;
        auto queryId = deployedQuery.second;

        NES_INFO("MonitoringManager: Stopping query " << queryId << " for " << NES::toString(metricType));
        if (queryService->validateAndQueueStopRequest(queryId)) {
            if (sync && checkStoppedOrTimeout(queryId, catalogService, std::chrono::seconds(60))) {
                NES_INFO("MonitoringManager: Query " << queryId << "::" << NES::toString(metricType) << " terminated.");
            }
        } else {
            NES_ERROR("MonitoringManager: Query " << queryId << "::" << NES::toString(metricType)
                                                  << " failed to terminate in time.");
            success = false;
        }
    }
    if (success) {
        deployedMonitoringQueries.clear();
        NES_INFO("MonitoringManager: All running monitoring queries stopped successfully.");
    }
    return success;
}

bool MonitoringManager::checkStoppedOrTimeout(QueryId queryId,
                                              const QueryCatalogServicePtr& catalogService,
                                              std::chrono::seconds timeout) {
    auto timeoutInSec = std::chrono::seconds(timeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkStoppedOrTimeout: check query status for " << queryId);
        if (catalogService->getEntryForQuery(queryId)->getQueryStatus() == QueryStatus::Stopped) {
            NES_TRACE("checkStoppedOrTimeout: status for " << queryId << " reached stopped");
            return true;
        }
        NES_DEBUG("checkStoppedOrTimeout: status not reached for "
                  << queryId << " as status is=" << catalogService->getEntryForQuery(queryId)->getQueryStatusAsString());
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
    NES_TRACE("checkStoppedOrTimeout: expected status not reached within set timeout");
    return false;
}

bool MonitoringManager::waitForQueryToStart(QueryId queryId,
                                            const QueryCatalogServicePtr& catalogService,
                                            std::chrono::seconds timeout) {
    NES_INFO("MonitoringManager: wait till the query " << queryId << " gets into Running status.");
    auto start_timestamp = std::chrono::system_clock::now();

    while (std::chrono::system_clock::now() < start_timestamp + timeout) {
        auto queryCatalogEntry = catalogService->getEntryForQuery(queryId);
        if (!queryCatalogEntry) {
            NES_ERROR("MonitoringManager: unable to find the entry for query " << queryId << " in the query catalog.");
            return false;
        }
        NES_TRACE("MonitoringManager: Query " << queryId << " is now in status " << queryCatalogEntry->getQueryStatusAsString());
        QueryStatus::Value status = queryCatalogEntry->getQueryStatus();

        switch (queryCatalogEntry->getQueryStatus()) {
            case QueryStatus::Running: {
                return true;
            }
            case QueryStatus::Scheduling: break;
            default: {
                NES_ERROR("MonitoringManager: Query failed to start. Expected: Running but found "
                          + QueryStatus::toString(status));
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
    NES_ERROR("MonitoringManager: Starting query " << queryId << " has timed out.");
    return false;
}

}// namespace NES