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

#ifndef NES_COORDINATOR_INCLUDE_MONITORING_MONITORINGMANAGER_HPP_
#define NES_COORDINATOR_INCLUDE_MONITORING_MONITORINGMANAGER_HPP_

#include <Identifiers.hpp>
#include <Monitoring/Metrics/MetricType.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/MetricCollectorType.hpp>
#include <atomic>
#include <chrono>
#include <memory>
#include <set>
#include <unordered_map>

namespace NES {

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace Configurations

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

namespace Monitoring {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;

class NesCoordinator;
using NesCoordinatorPtr = std::shared_ptr<NesCoordinator>;

class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

/**
* @brief The MonitoringManager is responsible for managing all global metrics of all nodes in the topology.
*/
class MonitoringManager {
  public:
    /**
     * Ctor to create a MonitoringManger for a given topology. For communication the manager will use the corresponding RPC client.
     * @param topology the topology
     * @param metricStore the metric store
     * @param enableMonitoring flag to indicate if monitoring is enabled or not
     */
    MonitoringManager(TopologyPtr topology,
                      QueryServicePtr queryService,
                      QueryCatalogServicePtr catalogService,
                      MetricStorePtr metricStore,
                      bool enableMonitoring);

    /**
     * Ctor to create a MonitoringManger for a given topology. For communication the manager will use the corresponding RPC client.
     * @param topology the topology
     * @param enableMonitoring flag to indicate if monitoring is enabled or not
     */
    MonitoringManager(TopologyPtr topology,
                      QueryServicePtr queryService,
                      QueryCatalogServicePtr catalogService,
                      bool enableMonitoring);

    /**
     * Ctor to create a MonitoringManger for a given topology. For communication the manager will use the corresponding RPC client.
     * @param topology the topology
     * @param queryService: the query service
     * @param queryCatalogService: the query catalog service
     */
    MonitoringManager(TopologyPtr topology, QueryServicePtr queryService, QueryCatalogServicePtr queryCatalogService);
    MonitoringManager(const MonitoringManager&) = default;
    MonitoringManager(MonitoringManager&&) = default;
    //  -- Assignment --
    MonitoringManager& operator=(const MonitoringManager&) = default;
    MonitoringManager& operator=(MonitoringManager&&) = default;
    //  -- dtor --
    ~MonitoringManager();

    /**
     * @brief Register a monitoring plan for given nodes.
     * @param nodeId
     * @param monitoringPlan
     * @return True, if successful, else false
    */
    bool registerRemoteMonitoringPlans(const std::vector<uint64_t>& nodeIds, MonitoringPlanPtr monitoringPlan);

    /**
     * @brief Get the monitoring data for a given node.
     * Note: Multiple nodes are not possible, as every node can have a different monitoring plan and
     * TupleBuffer is not supporting different nested schemas.
     * @param nodeId
     * @param tupleBuffer
     * @return the grouped metric values
    */
    nlohmann::json requestRemoteMonitoringData(uint64_t nodeId);

    /**
     * @brief Requests monitoring data from metric store.
     * @param nodeId
     * @return the grouped metric values
    */
    StoredNodeMetricsPtr getMonitoringDataFromMetricStore(uint64_t nodeId);

    /**
     * @brief Receive arbitrary monitoring data from a given node.
     * @param nodeId
     * @param GroupedMetricValuesPtr the grouped metric values
    */
    void addMonitoringData(uint64_t nodeId, MetricPtr metrics);

    /**
     * @brief Remove node from monitoring store.
     * @param nodeId
    */
    void removeMonitoringNode(uint64_t nodeId);

    /**
     * @brief Get the monitoring plan for a given node ID. If the node exists in the topology but has not a registered
     * plan, MonitoringPlan::Default will be returned. If the node does not exist an NES exception is thrown.
     * @param nodeId
     * @return The monitoring plan
    */
    MonitoringPlanPtr getMonitoringPlan(uint64_t nodeId);

    /**
     * @brief Registers the logical monitoring streams at the coordinator.
     * @return true if monitoring is disabled or if the streams have been registered successfully, else false
     */
    bool registerLogicalMonitoringStreams(const NES::Configurations::CoordinatorConfigurationPtr config);

    /**
     * @brief Starts or redeploys monitoring queries at the coordinator
     * @return true if successful, else false
     */
    std::unordered_map<std::string, QueryId> startOrRedeployMonitoringQueries(bool sync);

    /**
     * @brief Starts or redeploys monitoring queries at the coordinator
     * @param the logical stream name of the monitoring stream
     * @param bool true if it should block, else false
     * @return the QueryID the of the monitoring stream
     */
    QueryId startOrRedeployMonitoringQuery(std::string monitoringStream, bool sync);

    /**
     * @brief Checks if the logical stream is a monitoring stream
     * @param streamName
     * @return true if monitoring stream, else false
     */
    bool isMonitoringStream(std::string streamName) const;

    /**
     * @brief Stops a given running monitoring query;
     * @param the name of the monitoring stream
     * @param sync if it should block or not
     * @return true if success
     */
    bool stopRunningMonitoringQuery(std::string streamName, bool sync);

    /**
     * @brief Stops all running monitoring queries;
     * @param sync if to true then block
     * @return true if success
     */
    bool stopRunningMonitoringQueries(bool sync);

    /**
     * Getter for the metric store
     * @return the metric store
     */
    MetricStorePtr getMetricStore();

    /**
     * @brief Get the deployed monitoring queries
     * @return A map logicalStreamName -> QueryId
     */
    const std::unordered_map<std::string, QueryId>& getDeployedMonitoringQueries() const;

  private:
    bool waitForQueryToStart(QueryId queryId, std::chrono::seconds timeout);
    bool checkStoppedOrTimeout(QueryId queryId, std::chrono::seconds timeout);

  private:
    MetricStorePtr metricStore;
    std::unordered_map<uint64_t, MonitoringPlanPtr> monitoringPlanMap;
    std::unordered_map<std::string, QueryId> deployedMonitoringQueries;
    WorkerRPCClientPtr workerClient;
    TopologyPtr topology;
    bool enableMonitoring;
    std::set<MetricCollectorType> monitoringCollectors;
    std::set<std::string> logicalMonitoringSources;
    NES::QueryServicePtr queryService;
    NES::QueryCatalogServicePtr catalogService;
};

using MonitoringManagerPtr = std::shared_ptr<MonitoringManager>;

}// namespace Monitoring
}// namespace NES
#endif  // NES_COORDINATOR_INCLUDE_MONITORING_MONITORINGMANAGER_HPP_
