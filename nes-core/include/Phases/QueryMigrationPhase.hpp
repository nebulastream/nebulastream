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

#ifndef NES_CORE_INCLUDE_PHASES_QUERYMIGRATIONPHASE_HPP_
#define NES_CORE_INCLUDE_PHASES_QUERYMIGRATIONPHASE_HPP_
#include <memory>
#include <vector>

namespace NES::Optimizer {
class QueryPlacementPhase;
using QueryPlacementPhasePtr = std::shared_ptr<QueryPlacementPhase>;
}

namespace NES {

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Experimental {

class QueryMigrationPhase;
using QueryMigrationPhasePtr = std::shared_ptr<QueryMigrationPhase>;

class MigrateQueryRequest;
using MigrateQueryRequestPtr = std::shared_ptr<MigrateQueryRequest>;

class QueryMigrationPhase {

  public:
    /**
     * This method creates an instance of query migration phase
     * @param globalExecutionPlan : an instance of global execution plan
     * @param workerRPCClient : an instance of WorkerRPCClient, used to send buffer/reconfigure requests
     * @param queryPlacementPhase  : a query placement phase instance, used to do partial placement
     * @return pointer to query migration phase
     */
    static QueryMigrationPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan,
                                         TopologyPtr topology,
                                         WorkerRPCClientPtr workerRPCClient,
                                         NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase
                                         );

    /**
     * @brief Method takes MigrateQueryRequest and processes it
     * @return true if migration successful.
     */
    bool execute(const Experimental::MigrateQueryRequestPtr& migrateQueryRequest);

  private:
    explicit QueryMigrationPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                 TopologyPtr topology,
                                 WorkerRPCClientPtr workerRPCClient,
                                 NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase);

    bool executeMigrationWithBuffer(std::vector<QueryPlanPtr>& queryPlans, const ExecutionNodePtr& markedNode);

    bool executeMigrationWithoutBuffer(const std::vector<QueryPlanPtr>& queryPlans, const ExecutionNodePtr& markedNode);

    std::vector<TopologyNodePtr> findPath(unsigned long queryId, unsigned long topologyNodeId);

    std::vector<TopologyNodePtr> findChildExecutionNodesAsTopologyNodes(unsigned long queryId, unsigned long topologyNodeId);

    TopologyNodePtr findNewNodeLocationOfNetworkSource(unsigned long queryId,
                                                       unsigned long sourceOperatorId,
                                                       std::vector<ExecutionNodePtr>& potentialLocations);

    std::vector<TopologyNodePtr> findParentExecutionNodesAsTopologyNodes(unsigned long queryId, unsigned long topologyNodeId);

    bool deployQuery(unsigned long queryId, const std::vector<ExecutionNodePtr>& executionNodes);

    std::vector<ExecutionNodePtr> executePartialPlacement(QueryPlanPtr queryPlan);

    bool startQuery(unsigned long queryId, const std::vector<ExecutionNodePtr>& executionNodes);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    WorkerRPCClientPtr workerRPCClient;
    NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase;


};
} // namespace Experimental
} // namespace NES
#endif//NES_CORE_INCLUDE_PHASES_QUERYMIGRATIONPHASE_HPP_
