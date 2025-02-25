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

DeploymentContextPtr DeploymentContext::create(const std::string& grpcAddress,
                                               const DecomposedQueryPlanPtr& decomposedQueryPlan) {
    return std::make_shared<DeploymentContext>(grpcAddress,
                                               decomposedQueryPlan->getSharedQueryId(),
                                               decomposedQueryPlan->getDecomposedQueryId(),
                                               decomposedQueryPlan->getVersion(),
                                               decomposedQueryPlan->getWorkerId(),
                                               decomposedQueryPlan->getState(),
                                               decomposedQueryPlan);
}

DeploymentContext::DeploymentContext(const std::string& grpcAddress,
                                     SharedQueryId sharedQueryId,
                                     DecomposedQueryId decomposedQueryId,
                                     DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                     WorkerId workerId,
                                     QueryState decomposedQueryState,
                                     const DecomposedQueryPlanPtr& decomposedQueryPlan)
    : grpcAddress(grpcAddress), sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId),
      decomposedQueryPlanVersion(decomposedQueryPlanVersion), workerId(workerId), decomposedQueryState(decomposedQueryState),
      decomposedQueryPlan(decomposedQueryPlan) {}

SharedQueryId DeploymentContext::getSharedQueryId() { return sharedQueryId; }

QueryState DeploymentContext::getDecomposedQueryPlanState() { return decomposedQueryState; }

DecomposedQueryId DeploymentContext::getDecomposedQueryId() { return decomposedQueryId; };

std::string DeploymentContext::getGrpcAddress() const { return grpcAddress; }

DecomposedQueryPlanPtr DeploymentContext::getDecomposedQueryPlan() { return decomposedQueryPlan; }

DecomposedQueryPlanVersion DeploymentContext::getDecomposedQueryPlanVersion() const { return decomposedQueryPlanVersion; }

WorkerId DeploymentContext::getWorkerId() const { return workerId; }

void DeploymentContext::setForMigration(bool flag) {
    migration = flag;
}

bool DeploymentContext::isForMigration() {
    return migration;
}

}// namespace NES::Optimizer
