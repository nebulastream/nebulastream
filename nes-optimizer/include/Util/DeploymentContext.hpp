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

#ifndef NES_OPTIMIZER_INCLUDE_UTIL_DEPLOYMENTCONTEXT_HPP_
#define NES_OPTIMIZER_INCLUDE_UTIL_DEPLOYMENTCONTEXT_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Util/QueryState.hpp>
#include <memory>

namespace NES {

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

namespace Optimizer {

class DeploymentContext;
using DeploymentContextPtr = std::shared_ptr<DeploymentContext>;

class DeploymentContext {

  public:
    static DeploymentContextPtr create(const std::string& grpcAddress, const DecomposedQueryPlanPtr& decomposedQueryPlan);

    DeploymentContext(const std::string& grpcAddress,
                      SharedQueryId sharedQueryId,
                      DecomposedQueryId decomposedQueryId,
                      DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                      WorkerId workerId,
                      QueryState decomposedQueryState,
                      const DecomposedQueryPlanPtr& decomposedQueryPlan);

    /**
     * @brief Get the id of the shared query plan
     * @return the shared query id
     */
    SharedQueryId getSharedQueryId();

    /**
     * @brief Get the id of the decomposed query plan
     * @return decomposed query plan id
     */
    DecomposedQueryId getDecomposedQueryId();

    /**
     * @brief Get the decomposed query plan version
     * @return get the version
     */
    DecomposedQueryPlanVersion getDecomposedQueryPlanVersion() const;

    /**
     * @brief Get the worker id
     * @return get the worker id
     */
    WorkerId getWorkerId() const;

    /**
     * @brief Get the decomposed query plan state
     * @return query plan state
     */
    QueryState getDecomposedQueryPlanState();

    /**
     * @brief Get decomposed query plan to deploy
     * @return decomposed query plan
     */
    DecomposedQueryPlanPtr getDecomposedQueryPlan();

    /**
     * @brief GRPC address of the worker
     * @return string representing the grpc address of the worker
     */
    std::string getGrpcAddress() const;

    void setForMigration(bool flag);

    bool isForMigration();

  private:
    bool migration{false};
    std::string grpcAddress;
    SharedQueryId sharedQueryId;
    DecomposedQueryId decomposedQueryId;
    DecomposedQueryPlanVersion decomposedQueryPlanVersion;
    WorkerId workerId;
    QueryState decomposedQueryState;
    DecomposedQueryPlanPtr decomposedQueryPlan;
};
}// namespace Optimizer
}// namespace NES
#endif// NES_OPTIMIZER_INCLUDE_UTIL_DEPLOYMENTCONTEXT_HPP_
