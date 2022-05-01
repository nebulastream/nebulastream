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
namespace NES {

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

namespace NES::Optimizer {
class QueryPlacementPhase;
using QueryPlacementPhasePtr = std::shared_ptr<QueryPlacementPhase>;
}

namespace Experimental {

class QueryMigrationPhase;
using QueryMigrationPhasePtr = std::shared_ptr<QueryMigrationPhase>;

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
                                         WorkerRPCClientPtr workerRPCClient,
                                         NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase
                                         );

    /**
     * @brief Method takes input as a placement strategy name and input query plan and performs query operator placement based on the
     * selected query placement strategy
     * @param placementStrategy : name of the placement strategy
     * @param sharedQueryPlan : the shared query plan to place
     * @return true is placement successful.
     * @throws QueryPlacementException
     */
    bool execute();

  private:
    explicit QueryMigrationPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                 WorkerRPCClientPtr workerRPCClient,
                                 NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase);


  private:
    GlobalExecutionPlanPtr globalExecutionPlan;
    WorkerRPCClientPtr workerRPCClient;
    NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase;
};
}//nampespace Experimental
} // namespace NES
#endif//NES_CORE_INCLUDE_PHASES_QUERYMIGRATIONPHASE_HPP_
