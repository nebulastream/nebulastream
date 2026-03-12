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

#include <Serialization/QueryPlanFingerprintUtil.hpp>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Serialization/OperatorSerializationUtil.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
constexpr std::string_view ReplayRestoreBoundaryOperatorType = "ReplayRestoreBoundary";

[[nodiscard]] bool isReplayRestoreStatefulOperator(const LogicalOperator& op)
{
    return op.tryGetAs<JoinLogicalOperator>().has_value() || op.tryGetAs<WindowedAggregationLogicalOperator>().has_value();
}

[[nodiscard]] std::string computeFingerprintFromBytes(const std::string_view bytes)
{
    constexpr uint64_t fnvOffsetBasis = 1469598103934665603ULL;
    constexpr uint64_t fnvPrime = 1099511628211ULL;

    uint64_t hash = fnvOffsetBasis;
    for (const auto byte : bytes)
    {
        hash ^= static_cast<uint8_t>(byte);
        hash *= fnvPrime;
    }
    return fmt::format("fnv1a64:{:016x}", hash);
}

template <typename ProtoMessage>
[[nodiscard]] std::string serializeProtoDeterministically(const ProtoMessage& message)
{
    std::string bytes;
    bytes.reserve(static_cast<size_t>(message.ByteSizeLong()));
    google::protobuf::io::StringOutputStream rawOutput(&bytes);
    google::protobuf::io::CodedOutputStream codedOutput(&rawOutput);
    codedOutput.SetSerializationDeterministic(true);
    if (!message.SerializeToCodedStream(&codedOutput))
    {
        throw CannotSerialize("Failed to deterministically serialize canonical query plan for fingerprinting");
    }
    return bytes;
}

[[nodiscard]] SerializableQueryPlan canonicalizePlanForFingerprint(const SerializableQueryPlan& serializedPlan)
{
    std::vector<ReflectedOperator> reflectedOperators;
    reflectedOperators.reserve(serializedPlan.reflectedoperators_size());
    std::unordered_map<OperatorId::Underlying, const ReflectedOperator*> operatorsById;
    operatorsById.reserve(serializedPlan.reflectedoperators_size());
    for (const auto& reflectedOperator : serializedPlan.reflectedoperators())
    {
        auto parsedOperator = rfl::json::read<ReflectedOperator>(reflectedOperator);
        if (!parsedOperator.has_value())
        {
            throw CannotDeserialize("Failed to parse serialized operator while canonicalizing query plan fingerprint");
        }
        reflectedOperators.push_back(std::move(parsedOperator.value()));
        operatorsById.emplace(reflectedOperators.back().operatorId, &reflectedOperators.back());
    }

    std::unordered_map<OperatorId::Underlying, OperatorId::Underlying> canonicalIds;
    canonicalIds.reserve(operatorsById.size());
    OperatorId::Underlying nextOperatorId = INITIAL_OPERATOR_ID.getRawValue();
    std::vector<OperatorId::Underlying> worklist;
    worklist.reserve(serializedPlan.rootoperatorids_size() + reflectedOperators.size());
    for (const auto rootOperatorId : serializedPlan.rootoperatorids())
    {
        worklist.push_back(rootOperatorId);
    }

    for (size_t idx = 0; idx < worklist.size(); ++idx)
    {
        const auto originalOperatorId = worklist[idx];
        if (canonicalIds.contains(originalOperatorId))
        {
            continue;
        }
        canonicalIds.emplace(originalOperatorId, nextOperatorId++);
        if (const auto opIt = operatorsById.find(originalOperatorId); opIt != operatorsById.end())
        {
            for (const auto childId : opIt->second->childrenIds)
            {
                worklist.push_back(childId);
            }
        }
    }

    std::vector<OperatorId::Underlying> remainingOperatorIds;
    remainingOperatorIds.reserve(operatorsById.size());
    for (const auto& [operatorId, _] : operatorsById)
    {
        if (!canonicalIds.contains(operatorId))
        {
            remainingOperatorIds.push_back(operatorId);
        }
    }
    std::ranges::sort(remainingOperatorIds);
    for (const auto operatorId : remainingOperatorIds)
    {
        canonicalIds.emplace(operatorId, nextOperatorId++);
    }

    std::vector<ReflectedOperator> canonicalOperators;
    canonicalOperators.reserve(reflectedOperators.size());
    for (const auto& op : reflectedOperators)
    {
        auto canonicalOperator = op;
        canonicalOperator.operatorId = canonicalIds.at(op.operatorId);
        for (auto& childId : canonicalOperator.childrenIds)
        {
            childId = canonicalIds.at(childId);
        }
        canonicalOperators.push_back(std::move(canonicalOperator));
    }
    std::ranges::sort(
        canonicalOperators,
        [](const auto& lhs, const auto& rhs) { return lhs.operatorId < rhs.operatorId; });

    auto canonicalPlan = serializedPlan;
    canonicalPlan.clear_rootoperatorids();
    canonicalPlan.clear_reflectedoperators();
    for (const auto rootOperatorId : serializedPlan.rootoperatorids())
    {
        canonicalPlan.add_rootoperatorids(canonicalIds.at(rootOperatorId));
    }
    for (const auto& op : canonicalOperators)
    {
        canonicalPlan.add_reflectedoperators(rfl::json::write(op));
    }
    return canonicalPlan;
}

[[nodiscard]] ReflectedOperator serializeOperatorForReplayRestoreFingerprint(const LogicalOperator& current)
{
    if (const auto sink = current.tryGetAs<SinkLogicalOperator>(); sink.has_value() && !sink->get().getSinkDescriptor().has_value())
    {
        ReflectedOperator reflectedOperator;
        reflectedOperator.operatorId = current.getId().getRawValue();
        reflectedOperator.type = current.getName();
        reflectedOperator.inputSchemas = current.getInputSchemas();
        reflectedOperator.outputSchema = Schema{};
        reflectedOperator.traitSet = current.getTraitSet();
        for (const auto& child : current.getChildren())
        {
            reflectedOperator.childrenIds.push_back(child.getId().getRawValue());
        }
        reflectedOperator.config = current->reflect();
        return reflectedOperator;
    }

    return OperatorSerializationUtil::serializeOperator(current);
}

[[nodiscard]] bool replayRestoreSubtreeContainsStatefulOperator(
    const LogicalOperator& current,
    const std::unordered_map<OperatorId::Underlying, std::unordered_set<size_t>>& replayCheckpointBoundariesByParent)
{
    if (isReplayRestoreStatefulOperator(current))
    {
        return true;
    }

    const auto children = current.getChildren();
    const auto boundaryIt = replayCheckpointBoundariesByParent.find(current.getId().getRawValue());
    for (size_t childIndex = 0; childIndex < children.size(); ++childIndex)
    {
        const auto isBoundary
            = boundaryIt != replayCheckpointBoundariesByParent.end() && boundaryIt->second.contains(childIndex);
        if (isBoundary)
        {
            continue;
        }
        if (replayRestoreSubtreeContainsStatefulOperator(children[childIndex], replayCheckpointBoundariesByParent))
        {
            return true;
        }
    }

    return false;
}

void appendReplayRestoreFingerprintOperators(
    const LogicalOperator& current,
    const std::unordered_map<OperatorId::Underlying, std::unordered_set<size_t>>& replayCheckpointBoundariesByParent,
    OperatorId::Underlying& nextSyntheticOperatorId,
    SerializableQueryPlan& serializedPlan)
{
    auto reflectedOperator = serializeOperatorForReplayRestoreFingerprint(current);
    reflectedOperator.childrenIds.clear();
    const auto children = current.getChildren();

    for (size_t childIndex = 0; childIndex < children.size(); ++childIndex)
    {
        const auto child = children[childIndex];
        const auto boundaryIt = replayCheckpointBoundariesByParent.find(current.getId().getRawValue());
        const auto isBoundary
            = boundaryIt != replayCheckpointBoundariesByParent.end() && boundaryIt->second.contains(childIndex);
        if (isBoundary)
        {
            const auto boundaryOperatorId = nextSyntheticOperatorId++;
            reflectedOperator.childrenIds.push_back(boundaryOperatorId);
            serializedPlan.add_reflectedoperators(rfl::json::write(ReflectedOperator{
                .type = std::string(ReplayRestoreBoundaryOperatorType),
                .operatorId = boundaryOperatorId,
                .childrenIds = {},
                .config = Reflected{},
                .traitSet = TraitSet{},
                .inputSchemas = {},
                .outputSchema = child.getOutputSchema()}));
            continue;
        }

        appendReplayRestoreFingerprintOperators(
            child, replayCheckpointBoundariesByParent, nextSyntheticOperatorId, serializedPlan);
        reflectedOperator.childrenIds.push_back(child.getId().getRawValue());
    }

    serializedPlan.add_reflectedoperators(rfl::json::write(reflectedOperator));
}
}

std::string computePlanFingerprint(const SerializableQueryPlan& serializedPlan)
{
    auto canonicalPlan = canonicalizePlanForFingerprint(serializedPlan);
    canonicalPlan.clear_queryid();
    return computeFingerprintFromBytes(serializeProtoDeterministically(canonicalPlan));
}

std::string computePlanFingerprint(const LogicalPlan& plan)
{
    return computePlanFingerprint(QueryPlanSerializationUtil::serializeQueryPlan(plan));
}

std::optional<std::string>
computeReplayRestoreFingerprint(const LogicalPlan& plan, const std::vector<ReplayCheckpointBoundary>& replayCheckpointBoundaries)
{
    if (plan.getRootOperators().size() != 1U)
    {
        throw UnsupportedQuery(
            "Replay checkpoint planning requires a single-root local plan, but found {} roots",
            plan.getRootOperators().size());
    }

    std::unordered_map<OperatorId::Underlying, std::unordered_set<size_t>> replayCheckpointBoundariesByParent;
    replayCheckpointBoundariesByParent.reserve(replayCheckpointBoundaries.size());
    for (const auto& boundary : replayCheckpointBoundaries)
    {
        replayCheckpointBoundariesByParent[boundary.parentOperatorId.getRawValue()].insert(boundary.childIndex);
    }
    if (!replayRestoreSubtreeContainsStatefulOperator(plan.getRootOperators().front(), replayCheckpointBoundariesByParent))
    {
        return std::nullopt;
    }

    SerializableQueryPlan serializedPlan;
    serializedPlan.add_rootoperatorids(plan.getRootOperators().front().getId().getRawValue());
    OperatorId::Underlying nextSyntheticOperatorId = INITIAL_OPERATOR_ID.getRawValue();
    for (const auto& root : plan.getRootOperators())
    {
        for (const auto& current : BFSRange(root))
        {
            nextSyntheticOperatorId = std::max(nextSyntheticOperatorId, current.getId().getRawValue() + 1U);
        }
    }

    appendReplayRestoreFingerprintOperators(
        plan.getRootOperators().front(),
        replayCheckpointBoundariesByParent,
        nextSyntheticOperatorId,
        serializedPlan);
    return computePlanFingerprint(serializedPlan);
}

}
