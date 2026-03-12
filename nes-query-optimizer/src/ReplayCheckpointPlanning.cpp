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

#include <ReplayCheckpointPlanning.hpp>

#include <algorithm>
#include <ranges>
#include <unordered_map>
#include <vector>

#include <Iterators/BFSIterator.hpp>
#include <Serialization/QueryPlanFingerprintUtil.hpp>

namespace NES
{

std::expected<std::vector<ReplayCheckpointBoundary>, Exception>
buildReplayCheckpointBoundaries(const QueryRecordingPlanRewrite& queryPlanRewrite)
{
    std::unordered_map<OperatorId, LogicalOperator> operatorsById;
    for (const auto& root : queryPlanRewrite.basePlan.getRootOperators())
    {
        for (const auto& current : BFSRange(root))
        {
            operatorsById.insert_or_assign(current.getId(), current);
        }
    }

    std::vector<ReplayCheckpointBoundary> boundaries;
    for (const auto& insertion : queryPlanRewrite.insertions)
    {
        for (const auto& edge : insertion.materializationEdges)
        {
            const auto parentIt = operatorsById.find(edge.parentId);
            if (parentIt == operatorsById.end())
            {
                return std::unexpected(UnsupportedQuery(
                    "Replay checkpoint planning could not resolve replay boundary parent operator {}",
                    edge.parentId));
            }

            const auto& children = parentIt->second.getChildren();
            const auto childIt = std::ranges::find(children, edge.childId, &LogicalOperator::getId);
            if (childIt == children.end())
            {
                return std::unexpected(UnsupportedQuery(
                    "Replay checkpoint planning could not resolve replay boundary edge {} -> {}",
                    edge.parentId,
                    edge.childId));
            }

            boundaries.push_back(ReplayCheckpointBoundary{
                .parentOperatorId = edge.parentId,
                .childIndex = static_cast<size_t>(std::distance(children.begin(), childIt))});
        }
    }

    std::ranges::sort(
        boundaries,
        [](const auto& lhs, const auto& rhs)
        {
            if (lhs.parentOperatorId != rhs.parentOperatorId)
            {
                return lhs.parentOperatorId < rhs.parentOperatorId;
            }
            return lhs.childIndex < rhs.childIndex;
        });
    boundaries.erase(std::unique(boundaries.begin(), boundaries.end()), boundaries.end());
    return boundaries;
}

std::expected<ReplayCheckpointFingerprintsByHost, Exception>
computeReplayCheckpointFingerprints(
    const DistributedLogicalPlan& distributedPlan,
    const std::vector<ReplayCheckpointBoundary>& replayCheckpointBoundaries)
{
    ReplayCheckpointFingerprintsByHost replayCheckpointFingerprintsByHost;
    for (const auto& [host, localPlans] : distributedPlan)
    {
        auto& fingerprints = replayCheckpointFingerprintsByHost[host];
        fingerprints.reserve(localPlans.size());
        for (const auto& localPlan : localPlans)
        {
            try
            {
                fingerprints.push_back(computeReplayRestoreFingerprint(localPlan, replayCheckpointBoundaries));
            }
            catch (const Exception& exception)
            {
                return std::unexpected(exception);
            }
        }
    }
    return replayCheckpointFingerprintsByHost;
}

std::vector<ReplayCheckpointRequirement>
flattenReplayCheckpointRequirements(const ReplayCheckpointFingerprintsByHost& replayCheckpointFingerprintsByHost)
{
    auto hosts = replayCheckpointFingerprintsByHost | std::views::keys | std::ranges::to<std::vector>();
    std::ranges::sort(hosts);

    std::vector<ReplayCheckpointRequirement> requirements;
    for (const auto& host : hosts)
    {
        const auto& fingerprints = replayCheckpointFingerprintsByHost.at(host);
        for (const auto& fingerprint : fingerprints)
        {
            if (!fingerprint.has_value())
            {
                continue;
            }
            requirements.push_back(ReplayCheckpointRequirement{
                .host = host.getRawValue(),
                .replayRestoreFingerprint = *fingerprint});
        }
    }
    return requirements;
}

}
