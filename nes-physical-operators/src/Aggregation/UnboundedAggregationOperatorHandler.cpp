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
    SourcesOfInputOrigins sourcesOfInputOrigins, const OriginId outputOriginId, std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore)
    : AggregationOperatorHandler(std::move(sourcesOfInputOrigins), outputOriginId, std::move(sliceAndWindowStore))
{
}

void UnboundedAggregationOperatorHandler::parkDelta(const SequenceNumber sequenceNumber, const TupleBuffer& delta, const Timestamp sliceEnd)
{
    const auto [_, inserted] = applyState.wlock()->parkedDeltas.emplace(sequenceNumber, ParkedDelta{.delta = delta, .sliceEnd = sliceEnd});
    INVARIANT(inserted, "A delta was already parked for the sequence number {}", sequenceNumber);
}

bool UnboundedAggregationOperatorHandler::arriveAndTryToApply(const SequenceNumber sequenceNumber, const Timestamp watermark)
{
    const auto state = applyState.wlock();
    const auto [_, inserted] = state->arrived.emplace(sequenceNumber, watermark);
    INVARIANT(inserted, "The sequence number {} arrived twice at the unbounded aggregation", sequenceNumber);
    if (state->applierActive)
    {
        return false;
    }
    state->applierActive = true;
    return true;
}

UnboundedAggregationOperatorHandler::Claimed UnboundedAggregationOperatorHandler::claimNext()
{
    const auto state = applyState.wlock();
    INVARIANT(state->applierActive, "Only the applier may claim the next sequence number");
    claimed.reset();
    const auto next = state->nextToApply;
    const auto arrived = state->arrived.find(next);
    if (arrived == state->arrived.end())
    {
        state->applierActive = false;
        return Claimed::NOTHING;
    }
    claimedBufferWatermark = arrived->second;
    state->arrived.erase(arrived);
    state->nextToApply = SequenceNumber{next.getRawValue() + 1};
    if (const auto parked = state->parkedDeltas.find(next); parked != state->parkedDeltas.end())
    {
        claimed = std::move(parked->second);
        state->parkedDeltas.erase(parked);
        return Claimed::DELTA;
    }
    return Claimed::NO_DELTA;
}

const TupleBuffer& UnboundedAggregationOperatorHandler::claimedDelta() const
{
    INVARIANT(claimed.has_value(), "No delta was claimed");
    return claimed->delta;
}

Timestamp UnboundedAggregationOperatorHandler::claimedSliceEnd() const
{
    INVARIANT(claimed.has_value(), "No delta was claimed");
    return claimed->sliceEnd;
}

Timestamp UnboundedAggregationOperatorHandler::claimedWatermark() const
{
    return claimedBufferWatermark;
}

bool UnboundedAggregationOperatorHandler::hasTable() const
{
    return table.has_value();
}

void UnboundedAggregationOperatorHandler::setTable(const TupleBuffer& newTable)
{
    INVARIANT(not table.has_value(), "The table of the unbounded aggregation was already created");
    table = newTable;
}

const TupleBuffer& UnboundedAggregationOperatorHandler::getTable() const
{
    INVARIANT(table.has_value(), "The table of the unbounded aggregation was not created yet");
    return *table;
}

}
