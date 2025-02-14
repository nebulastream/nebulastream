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

#ifndef NES_COORDINATOR_INCLUDE_PHASES_DEPLOYMENTPHASE_HPP_
#define NES_COORDINATOR_INCLUDE_PHASES_DEPLOYMENTPHASE_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Util/RequestType.hpp>
#include <iostream>
#include <memory>
#include <set>

namespace NES {

class ReconfigurationMarker;
using ReconfigurationMarkerPtr = std::shared_ptr<ReconfigurationMarker>;

namespace Optimizer {
class DeploymentContext;
using DeploymentContextPtr = std::shared_ptr<DeploymentContext>;

struct ReconfigurationMarkerUnit;
}// namespace Optimizer

namespace Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace Configurations

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class DeploymentPhase;
using DeploymentPhasePtr = std::shared_ptr<DeploymentPhase>;

namespace Catalogs::Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Catalogs::Query

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

/**
 * @brief This class contains logic to transmit a collection of query sub plans to the desired worker nodes.
 */
class DeploymentPhase {
  public:
    /**
     * @brief Returns a smart pointer to the QueryDeploymentPhase
     * @return shared pointer to the instance of QueryDeploymentPhase
     */
    static DeploymentPhasePtr create(const Catalogs::Query::QueryCatalogPtr& queryCatalog, uint64_t reconnectCounter = 0, bool replay = false);

    explicit DeploymentPhase(const Catalogs::Query::QueryCatalogPtr& queryCatalog, uint64_t reconnectCounter = 0, bool replay = false);

    /**
     * @brief method for deploying decomposed query plans in different states
     * @param deploymentContexts: the vector of deployment contexts containing the worker rpc address and decomposed query plan
     * @param requestType: request type
     * @throws ExecutionNodeNotFoundException: Unable to find ExecutionNodes where the query {sharedQueryId} is deployed
     */
    virtual void execute(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts, RequestType requestType);

    /**
     * @brief method for deploying reconfiguration markers
     * @param reconfigurationMarkerUnits: set of reconfiguration marker unit
     * @param reconfigurationMarker
     */
    virtual void execute(const std::set<Optimizer::ReconfigurationMarkerUnit>& reconfigurationMarkerUnits,
                         const ReconfigurationMarkerPtr& reconfigurationMarker);

    virtual ~DeploymentPhase() = default;

  private:
    /**
     * @brief method send query to nodes
     * @param sharedQueryId
     * @return bool indicating success
     * todo: #3821 change to specific exception
     * @throws QueryDeploymentException The bytecode list of classes implementing the UDF must contain the fully-qualified name of the UDF
     * @throws QueryDeploymentException: Error in call to Elegant acceleration service with code
     * @throws QueryDeploymentException: QueryDeploymentPhase : unable to find query sub plan with id
     */
    void registerOrStopDecomposedQueryPlan(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts,
                                           RequestType requestType);

    /**
     * @brief
     * @param deploymentContexts
     * @param requestType
     */
    void startOrUnregisterDecomposedQueryPlan(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts,
                                              NES::RequestType requestType);

    WorkerRPCClientPtr workerRPCClient;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    uint64_t reconnectCounter;
    bool replay = false;
};
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_PHASES_DEPLOYMENTPHASE_HPP_
