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

#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Util/DeploymentContext.hpp>

namespace NES::Optimizer {

DeploymentContextPtr
DeploymentContext::create(const std::string& ipAddress, uint32_t grpcPort, const DecomposedQueryPlanPtr& decomposedQueryPlan) {
    return std::make_shared<DeploymentContext>(ipAddress,
                                               grpcPort,
                                               decomposedQueryPlan->getSharedQueryId(),
                                               decomposedQueryPlan->getState(),
                                               decomposedQueryPlan->getDecomposedQueryPlanId(),
                                               decomposedQueryPlan);
}

DeploymentContext::DeploymentContext(const std::string& ipAddress,
                                     uint32_t grpcPort,
                                     SharedQueryId sharedQueryId,
                                     QueryState decomposedQueryState,
                                     DecomposedQueryPlanId decomposedQueryPlanId,
                                     const DecomposedQueryPlanPtr& decomposedQueryPlan)
    : ipAddress(ipAddress), grpcPort(grpcPort), sharedQueryId(sharedQueryId), decomposedQueryState(decomposedQueryState),
      decomposedQueryPlanId(decomposedQueryPlanId), decomposedQueryPlan(decomposedQueryPlan) {}

SharedQueryId DeploymentContext::getSharedQueryId() { return sharedQueryId; }

QueryState DeploymentContext::getDecomposedQueryPlanState() { return decomposedQueryState; }

DecomposedQueryPlanId DeploymentContext::getDecomposedQueryPlanId() { return decomposedQueryPlanId; };

std::string DeploymentContext::getGrpcAddress() const { return ipAddress + ":" + std::to_string(grpcPort); }

DecomposedQueryPlanPtr DeploymentContext::getDecomposedQueryPlan() { return decomposedQueryPlan; }

}// namespace NES::Optimizer