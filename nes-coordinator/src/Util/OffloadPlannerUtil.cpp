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

#include <Catalogs/Topology/TopologyNode.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/OffloadPlannerUtil.hpp>
#include <Util/Placement/PlacementConstants.hpp>

namespace NES::Optimizer {
OffloadPlanner::OffloadPlanner(const GlobalExecutionPlanPtr& globalExecutionPlan,
                               const TopologyPtr& topology,
                               const SharedQueryPlanPtr& sharedQueryPlan)
    : globalExecutionPlan(globalExecutionPlan), topology(topology), sharedQueryPlan(sharedQueryPlan) {}


bool OffloadPlanner::validateTargetNodeForRedundancy(WorkerId originWorkerId,
                                                     WorkerId targetWorkerId) {
    NES_DEBUG("OffloadPlanner::validateTargetNodeForRedundancy: start validation.");

    // 2. Find origin path and alternative path
    auto originPathWorkerIds = getOriginPathWorkerIds(originWorkerId);
    auto alternativePathWorkerIds = getAlternativePathWorkerIds();

    // 3. Check if targetWorkerId is on the same path as the alternative operators
    bool invalid = !checkNoCommonNode(targetWorkerId, alternativePathWorkerIds);

    return !invalid;
}

std::optional<WorkerId> OffloadPlanner::findNewTargetNodeInSamePath(WorkerId originWorkerId,
                                                                    WorkerId invalidTargetWorkerId) {
    NES_DEBUG("OffloadPlanner::findNewTargetNodeInSamePath: trying to find alternative in the same path.");

    auto originPath = getOriginPathWorkerIds(originWorkerId);
    auto alternativePath = getAlternativePathWorkerIds();

    originPath.erase(invalidTargetWorkerId);

    // Try to pick another node from originPath that meets criteria:
    // - Not common node with alternative path
    // - Has resources
    for (auto candidate : originPath) {
        if (checkNoCommonNode(candidate, alternativePath) && checkResources(candidate)) {
            NES_DEBUG("findNewTargetNodeInSamePath: Found a suitable node: {}", candidate.getRawValue());
            return candidate;
        }
    }

    NES_DEBUG("findNewTargetNodeInSamePath: No suitable node found on same path.");
    return std::nullopt;
}

std::optional<WorkerId> OffloadPlanner::findNewTargetNodeOnAlternativePath(WorkerId originWorkerId) {
    NES_DEBUG("OffloadPlanner::findNewTargetNodeOnAlternativePath: searching for a node on another path.");

    auto originPath = getOriginPathWorkerIds(originWorkerId);
    auto alternativePath = getAlternativePathWorkerIds();


    // Filter out paths that intersect with originPath or alternativePath
    std::vector<std::vector<WorkerId>> candidatePaths;
    for (auto& p : findAllAvailablePaths()) {
        bool intersectsOrigin = std::any_of(p.begin(), p.end(), [&](WorkerId wid) { return originPath.count(wid) > 0; });
        bool intersectsAlt = std::any_of(p.begin(), p.end(), [&](WorkerId wid) { return alternativePath.count(wid) > 0; });
        if (!intersectsOrigin && !intersectsAlt) {
            candidatePaths.push_back(p);
        }
    }

    for (auto& p : candidatePaths) {
        for (auto candidate : p) {
            if (checkResources(candidate)) {
                NES_DEBUG("findNewTargetNodeOnAlternativePath: Found candidate {}", candidate.getRawValue());
                return candidate;
            }
        }
    }

    NES_DEBUG("findNewTargetNodeOnAlternativePath: No suitable node found on alternative path.");
    return std::nullopt;
}

bool OffloadPlanner::tryCreatingNewLinkToEnableAlternativePath() {
    NES_DEBUG("OffloadPlanner::tryCreatingNewLinkToEnableAlternativePath: Attempting topology modification.");

    // TODO: Implement logic that:
    // 1. Identifies two nodes that if connected form a new path.
    // 2. Calls topology->addTopologyNodeAsChild() to create a new link.
    // This is complex and very query-specific. For now, let's just log and return false.

    NES_WARNING("tryCreatingNewLinkToEnableAlternativePath: Not implemented, returning false.");
    return false;
}

std::set<WorkerId> OffloadPlanner::findPathContainingNode(const std::vector<std::vector<TopologyNodePtr>>& allPaths,
                                                          WorkerId nodeId) {
    for (auto& path : allPaths) {
        std::set<WorkerId> pathIds;
        for (auto& node : path) {
            pathIds.insert(node->getId());
        }
        if (pathIds.count(nodeId) > 0) {
            return pathIds;
        }
    }

    return {};
}

std::set<WorkerId> OffloadPlanner::getOriginPathWorkerIds(WorkerId originWorkerId) {

    std::vector<WorkerId> originalPathWorkerIds;
    for (auto& wPath : findAllAvailablePaths()) {
        if (std::find(wPath.begin(), wPath.end(), originWorkerId) != wPath.end()) {
            originalPathWorkerIds = wPath;
            break;
        }
    }
    return std::set<WorkerId>(originalPathWorkerIds.begin(), originalPathWorkerIds.end());
}

std::set<WorkerId> OffloadPlanner::getAlternativePathWorkerIds() {

    WorkerId alternativeOperatorWorkerId = INVALID_WORKER_NODE_ID;
    {
        auto sinks = sharedQueryPlan->getSinkOperators();
        for (auto sink : sinks) {
            auto nextOps = sink->getChildren();
            for (auto nextOp : nextOps) {
                auto logOp = nextOp->as<LogicalOperator>();
                if (logOp->getOriginalId() != logOp->getId() && logOp->hasProperty(Optimizer::PINNED_WORKER_ID)) {
                    alternativeOperatorWorkerId = std::any_cast<WorkerId>(logOp->as<LogicalOperator>()->getProperty(Optimizer::PINNED_WORKER_ID));
                    break;
                }
            }
        }
    }

    std::vector<WorkerId> alternativePathWorkerIds;
    if (alternativeOperatorWorkerId != INVALID_WORKER_NODE_ID) {
        for (auto& wPath : findAllAvailablePaths()) {
            if (std::find(wPath.begin(), wPath.end(), alternativeOperatorWorkerId) != wPath.end()) {
                alternativePathWorkerIds = wPath;
                break;
            }
        }
    }
    return std::set<WorkerId>(alternativePathWorkerIds.begin(), alternativePathWorkerIds.end());
}

std::vector<std::vector<WorkerId>> OffloadPlanner::findAllAvailablePaths() {
    auto sourceOperators = sharedQueryPlan->getQueryPlan()->getLeafOperators();
    auto sinkOperators = sharedQueryPlan->getSinkOperators();
    std::vector<WorkerId> sourceIds, sinkIds;

    for (auto& op : sourceOperators) {
        if (op->hasProperty(PINNED_WORKER_ID)) {
            sourceIds.push_back(std::any_cast<WorkerId>(op->getProperty(PINNED_WORKER_ID)));
        }
    }
    for (auto& op : sinkOperators) {
        if (op->hasProperty(PINNED_WORKER_ID)) {
            sinkIds.push_back(std::any_cast<WorkerId>(op->getProperty(PINNED_WORKER_ID)));
        }
    }

    auto allPaths = topology->findAllPathsBetween(sourceIds, sinkIds);
    auto workerIdPaths = std::vector<std::vector<WorkerId>>();
    for (auto& path : allPaths) {
        std::vector<WorkerId> wPath;
        for (auto& node : path) wPath.push_back(node->getId());
        workerIdPaths.push_back(wPath);
    }
    return workerIdPaths;
}
bool OffloadPlanner::checkResources(WorkerId workerId) {
    if(topology->getCopyOfTopologyNodeWithId(workerId)->getAvailableResources()) {
        NES_DEBUG("OffloadPlanner::checkResources: Stub, returning true.");
        return true;
    }
    return false;
}

bool OffloadPlanner::checkNoCommonNode(WorkerId targetWorkerId,
                                       const std::set<WorkerId>& alternativePathWorkerIds) {

    if (alternativePathWorkerIds.count(targetWorkerId)) {
        NES_DEBUG("checkNoCommonNode: target {} is in alternative path.", targetWorkerId.getRawValue());
        return false;
    }
    NES_DEBUG("checkNoCommonNode: Passed checks.");
    return true;
}

std::pair<std::set<OperatorId>, std::set<OperatorId>>
OffloadPlanner::findUpstreamAndDownstreamPinnedOperators(WorkerId originWorkerId,
                                                         SharedQueryId sharedQueryId,
                                                         DecomposedQueryId decomposedQueryId,
                                         WorkerId targetWorkerId)
{
    auto decPlan = globalExecutionPlan->getCopyOfDecomposedQueryPlan(originWorkerId,
                                                                     sharedQueryId,
                                                                     decomposedQueryId);
    if (!decPlan || decPlan->getAllOperators().empty()) {
        NES_WARNING("No operators found for sub-plan on worker={}, sharedQuery={}, decomposedQuery={}",
                    originWorkerId.getRawValue(), sharedQueryId.getRawValue(), decomposedQueryId.getRawValue());
        return {};
    }

    auto anyOperatorInThisPlan = *decPlan->getAllOperators().begin();
    for (auto& candidateOp : decPlan->getAllOperators()) {
        auto logicalCandidate = candidateOp->as<LogicalOperator>();
        if ((logicalCandidate->instanceOf<SourceLogicalOperator>()
                             && logicalCandidate->as_if<SourceLogicalOperator>()
                                    ->getSourceDescriptor()
                                    ->instanceOf<Network::NetworkSourceDescriptor>())
                            || (logicalCandidate->instanceOf<SinkLogicalOperator>()
                                && logicalCandidate->as_if<SinkLogicalOperator>()
                                       ->getSinkDescriptor()
                                       ->instanceOf<Network::NetworkSinkDescriptor>())) {
            continue;
        }
        anyOperatorInThisPlan = logicalCandidate;
        break;
    }
    auto affectedOperator = sharedQueryPlan->getQueryPlan()
                           ->getOperatorWithOperatorId(anyOperatorInThisPlan->getId());
    if (!affectedOperator) {
        NES_WARNING("Affected operator not found in the main query plan. Returning empty sets.");
        return {};
    }

    std::set<OperatorId> upstreamDiff = findDifferentWorkerOperatorsUp(affectedOperator, originWorkerId, targetWorkerId);
    std::set<OperatorId> downstreamDiff = findDifferentWorkerOperatorsDown(affectedOperator, originWorkerId, targetWorkerId);

    return { upstreamDiff, downstreamDiff };
}

std::set<OperatorId>
OffloadPlanner::findDifferentWorkerOperatorsDown(const OperatorPtr& startOp,
                                WorkerId originWorkerId,
                                WorkerId targetWorkerId)
{
    std::set<OperatorId> offloadCandidates;
    if (!startOp) return offloadCandidates;

    std::queue<OperatorPtr> bfsQueue;
    std::unordered_set<OperatorId> visited;
    bfsQueue.push(startOp);

    while (!bfsQueue.empty()) {
        auto current = bfsQueue.front();
        bfsQueue.pop();

        if (!visited.insert(current->getId()).second) {
            continue;
        }

        if (current->hasProperty(Optimizer::PINNED_WORKER_ID)) {
            auto pinned = std::any_cast<WorkerId>(current->getProperty(Optimizer::PINNED_WORKER_ID));
            if (pinned == originWorkerId) {
                current->addProperty("WORKER_ID_TO_OFFLOAD", targetWorkerId);
                auto logOp = current->as<LogicalOperator>();
                if (logOp) {
                    for (auto& parent : logOp->getParents()) {
                        bfsQueue.push(parent->as<Operator>());
                    }
                }
            } else {
                NES_TRACE("Up BFS stopping at operator {} pinned to another worker {}",
                          current->getId(), pinned.getRawValue());
                offloadCandidates.insert(current->getId());
            }
        }
        else {
            NES_TRACE("No PINNED_WORKER_ID property on operator {}. BFS skipping.", current->getId());
        }
    }

    return offloadCandidates;
}

std::set<OperatorId>
OffloadPlanner::findDifferentWorkerOperatorsUp(const OperatorPtr& startOp,
                                  WorkerId originWorkerId,
                                  WorkerId targetWorkerId)
{
    std::set<OperatorId> offloadCandidates;
    if (!startOp) return offloadCandidates;

    std::queue<OperatorPtr> bfsQueue;
    std::unordered_set<OperatorId> visited;
    bfsQueue.push(startOp);

    while (!bfsQueue.empty()) {
        auto current = bfsQueue.front();
        bfsQueue.pop();

        if (!visited.insert(current->getId()).second) {
            continue;
        }

        if (current->hasProperty(Optimizer::PINNED_WORKER_ID)) {
            auto pinned = std::any_cast<WorkerId>(current->getProperty(Optimizer::PINNED_WORKER_ID));
            if (pinned == originWorkerId) {
                current->addProperty("WORKER_ID_TO_OFFLOAD", targetWorkerId);
                auto logOp = current->as<LogicalOperator>();
                if (logOp) {
                    for (auto& child : logOp->getChildren()) {
                        bfsQueue.push(child->as<Operator>());
                    }
                }
            } else {
                NES_TRACE("Down BFS stopping at operator {} pinned to another worker {}",
                          current->getId(), pinned.getRawValue());
                offloadCandidates.insert(current->getId());
            }
        }
        else {
            NES_TRACE("No PINNED_WORKER_ID property on operator {}. BFS skipping.", current->getId());
        }
    }

    return offloadCandidates;
}

}
