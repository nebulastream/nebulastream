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

#include <optional>
#include <Aggregation/UnboundedAggregationOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/Timestamp.hpp>

namespace NES
{

/// Handler of an aggregation with a SESSION window. It applies the deltas of its slices in slice order like the unbounded
/// aggregation (see UnboundedAggregationOperatorHandler); its table holds the open session of every key.
///
/// The slices are as wide as the gap, so the session of a key that a delta extends ends within one gap after the end of the delta's
/// slice. Once all records up to that point have been applied (the frontier), every session of the delta that no later delta
/// extended is complete. Only the keys of the last applied delta can therefore have an open session, and the handler keeps that
/// delta, the open sessions, until the frontier passes the end of its slice plus the gap.
class SessionWindowOperatorHandler final : public UnboundedAggregationOperatorHandler
{
public:
    SessionWindowOperatorHandler(
        SourcesOfInputOrigins sourcesOfInputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore);

    /// Only for the applier: the time below which all records have been claimed: the end of the claimed slice, or the watermark of
    /// a sequence number that carried a watermark only.
    [[nodiscard]] Timestamp claimedFrontier() const;

    /// Only for the applier: the last applied delta, whose keys may have open sessions, and the end of its slice.
    [[nodiscard]] bool hasOpenSessions() const;
    [[nodiscard]] const TupleBuffer& getOpenSessions() const;
    [[nodiscard]] Timestamp getOpenSessionsSliceEnd() const;
    void setOpenSessions(const TupleBuffer& delta, Timestamp sliceEnd);
    void clearOpenSessions();

private:
    std::optional<ParkedDelta> openSessions;
};

}
