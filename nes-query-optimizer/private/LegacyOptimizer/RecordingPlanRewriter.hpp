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
#pragma once

#include <unordered_map>

#include <Plans/LogicalPlan.hpp>
#include <RecordingSelectionResult.hpp>

namespace NES
{
struct RecordingRewriteEdgeHash
{
    [[nodiscard]] size_t operator()(const RecordingRewriteEdge& edge) const
    {
        return std::hash<uint64_t>{}(edge.parentId.getRawValue()) ^ (std::hash<uint64_t>{}(edge.childId.getRawValue()) << 1U);
    }
};

using RecordingSelectionsByEdge = std::unordered_map<RecordingRewriteEdge, RecordingSelection, RecordingRewriteEdgeHash>;

[[nodiscard]] LogicalPlan stripReplayStores(const LogicalPlan& plan);
[[nodiscard]] LogicalPlan rewriteReplayBoundary(const LogicalPlan& plan, const RecordingSelectionsByEdge& storesToInsert);
[[nodiscard]] LogicalPlan replaceSinksWithVoid(const LogicalPlan& plan);
}
