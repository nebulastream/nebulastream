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


#include <Aggregation/SessionWindowOperatorHandler.hpp>

#include <memory>
#include <utility>
#include <Aggregation/UnboundedAggregationOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

SessionWindowOperatorHandler::SessionWindowOperatorHandler(
    SourcesOfInputOrigins sourcesOfInputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore)
    : UnboundedAggregationOperatorHandler(std::move(sourcesOfInputOrigins), outputOriginId, std::move(sliceAndWindowStore))
{
}

Timestamp SessionWindowOperatorHandler::claimedFrontier() const
{
    /// The buffer of a slice carries the start of the slice as its watermark, but its records reach up to the end of the slice.
    const auto& claimed = partitions.front()->claimed;
    return claimed.has_value() ? claimed->sliceEnd : claimedWatermark();
}

bool SessionWindowOperatorHandler::hasOpenSessions() const
{
    return openSessions.has_value();
}

const TupleBuffer& SessionWindowOperatorHandler::getOpenSessions() const
{
    INVARIANT(openSessions.has_value(), "The session window has no open sessions");
    return openSessions->delta;
}

Timestamp SessionWindowOperatorHandler::getOpenSessionsSliceEnd() const
{
    INVARIANT(openSessions.has_value(), "The session window has no open sessions");
    return openSessions->sliceEnd;
}

void SessionWindowOperatorHandler::setOpenSessions(const TupleBuffer& delta, const Timestamp sliceEnd)
{
    INVARIANT(not openSessions.has_value(), "The open sessions of the session window were not closed before the next delta");
    openSessions = ParkedDelta{.delta = delta, .sliceEnd = sliceEnd};
}

void SessionWindowOperatorHandler::clearOpenSessions()
{
    openSessions.reset();
}

}
