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

#include <Aggregation/UnboundedAggregationOperatorHandler.hpp>

#include <cstdint>
#include <map>
#include <vector>
#include <memory>
#include <utility>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

UnboundedAggregationOperatorHandler::UnboundedAggregationOperatorHandler(
    SourcesOfInputOrigins sourcesOfInputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore,
    const uint64_t numberOfPartitions)
    : AggregationOperatorHandler(std::move(sourcesOfInputOrigins), outputOriginId, std::move(sliceAndWindowStore))
{
    PRECONDITION(numberOfPartitions > 0, "An unbounded aggregation needs at least one partition");
    partitions.reserve(numberOfPartitions);
    for (uint64_t partition = 0; partition < numberOfPartitions; ++partition)
    {
        partitions.emplace_back(std::make_unique<Partition>());
    }
}

SequenceNumber UnboundedAggregationOperatorHandler::partitionSequenceNumber(const SequenceNumber storeSequenceNumber, const uint64_t partition) const
{
    return SequenceNumber{((storeSequenceNumber.getRawValue() - SequenceNumber::INITIAL) * partitions.size()) + partition + SequenceNumber::INITIAL};
}

uint64_t UnboundedAggregationOperatorHandler::partitionOf(const SequenceNumber sequenceNumber) const
{
    return (sequenceNumber.getRawValue() - SequenceNumber::INITIAL) % partitions.size();
}

SequenceNumber UnboundedAggregationOperatorHandler::positionOf(const SequenceNumber sequenceNumber) const
{
    return SequenceNumber{((sequenceNumber.getRawValue() - SequenceNumber::INITIAL) / partitions.size()) + SequenceNumber::INITIAL};
}

void UnboundedAggregationOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        for (uint64_t partition = 0; partition < partitions.size(); ++partition)
        {
            emitWindow(windowInfo.windowInfo, partitionSequenceNumber(windowInfo.sequenceNumber, partition), allSlices, pipelineCtx);
        }
    }
}

void UnboundedAggregationOperatorHandler::emitWatermark(
    const Timestamp watermark, const SequenceNumber sequenceNumber, PipelineExecutionContext* pipelineCtx)
{
    for (uint64_t partition = 0; partition < partitions.size(); ++partition)
    {
        AggregationOperatorHandler::emitWatermark(watermark, partitionSequenceNumber(sequenceNumber, partition), pipelineCtx);
    }
}

void UnboundedAggregationOperatorHandler::parkDelta(const SequenceNumber sequenceNumber, const TupleBuffer& delta, const Timestamp sliceEnd)
{
    auto& partition = *partitions[partitionOf(sequenceNumber)];
    const auto [_, inserted]
        = partition.applyState.wlock()->parkedDeltas.emplace(positionOf(sequenceNumber), ParkedDelta{.delta = delta, .sliceEnd = sliceEnd});
    INVARIANT(inserted, "A delta was already parked for the sequence number {}", sequenceNumber);
}

bool UnboundedAggregationOperatorHandler::arriveAndTryToApply(const SequenceNumber sequenceNumber, const Timestamp watermark)
{
    auto& partition = *partitions[partitionOf(sequenceNumber)];
    const auto state = partition.applyState.wlock();
    const auto [_, inserted] = state->arrived.emplace(positionOf(sequenceNumber), watermark);
    INVARIANT(inserted, "The sequence number {} arrived twice at the unbounded aggregation", sequenceNumber);
    if (state->applierActive)
    {
        return false;
    }
    state->applierActive = true;
    return true;
}

UnboundedAggregationOperatorHandler::Claimed UnboundedAggregationOperatorHandler::claimNext(const uint64_t partitionIndex)
{
    auto& partition = *partitions.at(partitionIndex);
    const auto state = partition.applyState.wlock();
    INVARIANT(state->applierActive, "Only the applier may claim the next sequence number");
    partition.claimed.reset();
    const auto next = state->nextToApply;
    const auto arrived = state->arrived.find(next);
    if (arrived == state->arrived.end())
    {
        state->applierActive = false;
        return Claimed::NOTHING;
    }
    partition.claimedBufferWatermark = arrived->second;
    state->arrived.erase(arrived);
    state->nextToApply = SequenceNumber{next.getRawValue() + 1};
    if (const auto parked = state->parkedDeltas.find(next); parked != state->parkedDeltas.end())
    {
        partition.claimed = std::move(parked->second);
        state->parkedDeltas.erase(parked);
        return Claimed::DELTA;
    }
    return Claimed::NO_DELTA;
}

const TupleBuffer& UnboundedAggregationOperatorHandler::claimedDelta(const uint64_t partition) const
{
    const auto& claimed = partitions.at(partition)->claimed;
    INVARIANT(claimed.has_value(), "No delta was claimed");
    return claimed->delta;
}

Timestamp UnboundedAggregationOperatorHandler::claimedSliceEnd(const uint64_t partition) const
{
    const auto& claimed = partitions.at(partition)->claimed;
    INVARIANT(claimed.has_value(), "No delta was claimed");
    return claimed->sliceEnd;
}

Timestamp UnboundedAggregationOperatorHandler::claimedWatermark(const uint64_t partition) const
{
    return partitions.at(partition)->claimedBufferWatermark;
}

bool UnboundedAggregationOperatorHandler::hasTable(const uint64_t partition) const
{
    return partitions.at(partition)->table.has_value();
}

void UnboundedAggregationOperatorHandler::setTable(const TupleBuffer& newTable, const uint64_t partition)
{
    auto& table = partitions.at(partition)->table;
    INVARIANT(not table.has_value(), "The table of the unbounded aggregation was already created");
    table = newTable;
}

const TupleBuffer& UnboundedAggregationOperatorHandler::getTable(const uint64_t partition) const
{
    const auto& table = partitions.at(partition)->table;
    INVARIANT(table.has_value(), "The table of the unbounded aggregation was not created yet");
    return *table;
}

}
