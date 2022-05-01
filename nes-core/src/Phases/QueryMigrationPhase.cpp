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

#include <Phases/QueryMigrationPhase.hpp>
namespace NES {
Experimental::QueryMigrationPhase::QueryMigrationPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                           WorkerRPCClientPtr workerRpcClient,
                                           NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase)
    : globalExecutionPlan(std::move(globalExecutionPlan)), workerRPCClient(std::move(workerRpcClient)),
      queryPlacementPhase(std::move(queryPlacementPhase)) {}

Experimental::QueryMigrationPhasePtr
Experimental::QueryMigrationPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                               WorkerRPCClientPtr workerRPCClient,
                                               NES::Optimizer::QueryPlacementPhasePtr queryPlacementPhase) {
    return std::make_shared<QueryMigrationPhase>(QueryMigrationPhase(std::move(globalExecutionPlan), std::move(workerRPCClient), std::move(queryPlacementPhase)));
}
bool Experimental::QueryMigrationPhase::execute(const Experimental::MigrateQueryRequestPtr& migrateQueryRequest) {
    return false; }
} //namespace NES