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


#ifndef OFFLOADPLANNERUTIL_HPP
#define OFFLOADPLANNERUTIL_HPP
#include <Catalogs/Topology/Topology.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>

namespace NES::Optimizer {
class OffloadPlanner {
public:
    OffloadPlanner(const GlobalExecutionPlanPtr& globalExecutionPlan,
                   const TopologyPtr& topology,
                   const SharedQueryPlanPtr& sharedQueryPlan);

    bool validateTargetNodeForRedundancy(WorkerId originWorkerId,
                                         WorkerId targetWorkerId);

    std::optional<WorkerId> findNewTargetNodeInSamePath(WorkerId originWorkerId,
                                                        WorkerId invalidTargetWorkerId);

    std::optional<WorkerId> findNewTargetNodeOnAlternativePath(WorkerId originWorkerId);

    bool tryCreatingNewLinkToEnableAlternativePath();

    std::set<WorkerId> findPathContainingNode(const std::vector<std::vector<TopologyNodePtr>>& allPaths,
                                              WorkerId nodeId);

    std::pair<std::set<OperatorId>, std::set<OperatorId>>
findUpstreamAndDownstreamPinnedOperators(WorkerId originWorkerId,
                                         SharedQueryId sharedQueryId,
                                         DecomposedQueryId decomposedQueryId,
                                         WorkerId targetWorkerId);
private:
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    SharedQueryPlanPtr sharedQueryPlan;

    std::set<WorkerId> getOriginPathWorkerIds(WorkerId originWorkerId);
    std::set<WorkerId> getAlternativePathWorkerIds();
    bool checkResources(WorkerId workerId);
    bool checkNoCommonNode(WorkerId targetWorkerId,
                           const std::set<WorkerId>& alternativePathWorkerIds);
    std::vector<std::vector<WorkerId>> findAllAvailablePaths();
    std::set<OperatorId> findDifferentWorkerOperatorsUp(const OperatorPtr& startOp,
    WorkerId localWorker,
WorkerId targetWorkerId);
    std::set<OperatorId> findDifferentWorkerOperatorsDown(const OperatorPtr& startOp,
    WorkerId localWorker,
WorkerId targetWorkerId);
};

}
#endif //OFFLOADPLANNERUTIL_HPP
