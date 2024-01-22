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

#ifndef NES_DEPLOYMENTCONTEXT_HPP
#define NES_DEPLOYMENTCONTEXT_HPP

#include <Identifiers.hpp>
#include <memory>

namespace NES {

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

namespace Optimizer {

class DeploymentContext;
using DeploymentContextPtr = std::shared_ptr<DeploymentContext>;

class DeploymentContext {

  public:
    static DeploymentContextPtr
    create(const std::string& ipAddress, uint32_t grpcPort, const DecomposedQueryPlanPtr& decomposedQueryPlan);

    DeploymentContext(const std::string& ipAddress,
                      uint32_t grpcPort,
                      DecomposedQueryPlanId decomposedQueryPlanId,
                      const DecomposedQueryPlanPtr& decomposedQueryPlan);

    /**
     * @brief Get the id of the decomposed query plan
     * @return decomposed query plan id
     */
    DecomposedQueryPlanId getDecomposedQueryPlanId();

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

  private:
    std::string ipAddress;
    uint32_t grpcPort;
    DecomposedQueryPlanId decomposedQueryPlanId;
    DecomposedQueryPlanPtr decomposedQueryPlan;
};
}// namespace Optimizer
}// namespace NES
#endif//NES_DEPLOYMENTCONTEXT_HPP
