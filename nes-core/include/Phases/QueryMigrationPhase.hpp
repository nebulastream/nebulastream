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

#include <Common/Identifiers.hpp>
#include <map>
#include <memory>
#include <vector>

namespace NES::Optimizer {
class QueryPlacementPhase;
using QueryPlacementPhasePtr = std::shared_ptr<QueryPlacementPhase>;
}// namespace NES::Optimizer

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

class SourceLogicalOperatorNode;
using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;

namespace Experimental {

class QueryMigrationPhase;
using QueryMigrationPhasePtr = std::shared_ptr<QueryMigrationPhase>;

class MaintenanceRequest;
using MaintenanceRequestPtr = std::shared_ptr<MaintenanceRequest>;

/**
 * @brief The QueryMigrationPhase is responsible for handling MaintenanceRequests and QueryMigrationRequests
 *
 * Query Migration is made up of two parts: 1. Clean Up and 2. Migration
 * 1. Clean Up:
 *      Made up of decentralized and centralized component:
 * -Decentralized:
 *      QMP creates message that propagates across all nodes that need clean up. Once this message reaches the last node that needs clean up,
 *      that node sends an Ack to the Coordinator.
 * -Centralized:
 *      After receiving the Ack, the Query Migration Phase cleans up centralized data structures (Global Execution Plan)
 *
 * 2. Migration: Migration consists of 3 steps:
 *      -Placement
 *      -Deployment
 *      -Reconfiguration
 *
 * Depending on the Migration Type, the above phases can be triggered in different orders.
 */
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
                                         NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase);

    /**
     * @brief Method takes MigrateQueryRequest and processes it
     * @return true if migration successful.
     */
    bool execute(const Experimental::MaintenanceRequestPtr& maintenanceRequest);

  private:
    explicit QueryMigrationPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                 TopologyPtr topology,
                                 WorkerRPCClientPtr workerRPCClient,
                                 NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    WorkerRPCClientPtr workerRPCClient;
    NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase;
};
}//namespace Experimental
}//namespace NES
#endif// NES_CORE_INCLUDE_PHASES_QUERYMIGRATIONPHASE_HPP_
