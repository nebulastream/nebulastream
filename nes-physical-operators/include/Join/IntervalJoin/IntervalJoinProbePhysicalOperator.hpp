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

#include <cstdint>
#include <memory>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Time/IntervalBound.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Probe operator for the streaming interval join.
///
/// A probe buffer represents one driving ("anchor") slice with up to 3 partner slice ends. The two
/// null-fill flags — set at lowering from the join type — select the behaviour, so a single class
/// covers inner and all outer variants:
///   - both false: inner / cartesian — emit matched (anchor, partner) pairs only;
///   - emitAnchorNullFill (LEFT/FULL): additionally null-fill anchor tuples with no partner;
///   - emitPartnerNullFill (RIGHT/FULL): additionally run the partner-anchored null-fill pass.
///
/// Inherits StreamJoinProbePhysicalOperator for createJoinedRecord/createNullFilledJoinedRecord, but
/// overrides close() because the inherited WindowProbePhysicalOperator::close would dynamic_cast the
/// handler to WindowBasedOperatorHandler, which the interval-join handler does not extend.
class IntervalJoinProbePhysicalOperator final : public StreamJoinProbePhysicalOperator
{
public:
    IntervalJoinProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::unique_ptr<TimeFunction> anchorTimeFunction,
        std::unique_ptr<TimeFunction> partnerTimeFunction,
        IntervalBound lowerBound,
        IntervalBound upperBound,
        std::shared_ptr<PagedVectorTupleLayout> anchorTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> partnerTupleLayout,
        std::vector<Record::RecordFieldIdentifier> anchorKeyFieldNames,
        std::vector<Record::RecordFieldIdentifier> partnerKeyFieldNames,
        bool emitAnchorNullFill,
        bool emitPartnerNullFill);

    IntervalJoinProbePhysicalOperator(const IntervalJoinProbePhysicalOperator& other);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

protected:
    /// Common open() prologue: propagate buffer metadata onto the execution context and open the
    /// per-side time functions.
    void prepareOpen(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// Scans the buffer's driving slice against its partner slices. Two modes selected by `driverIsAnchor`
    /// (a compile-time flag, so each call site specializes and the inner probe generates no null-fill code):
    ///   - driverIsAnchor (anchor-driven pass): driver = anchor-store slice, partners = partner-store slices;
    ///     emits a joined row for every partner tuple inside the interval, and — when emitAnchorNullFill is set —
    ///     a partner-side-null row for any anchor tuple with no partner.
    ///   - !driverIsAnchor (partner-anchored null-fill pass, outer probe only): roles swap; matches were already
    ///     emitted by the anchor-driven pass, so this only emits an anchor-side-null row for each partner tuple
    ///     that matched no anchor tuple.
    void runJoinPass(ExecutionContext& executionCtx, RecordBuffer& recordBuffer, bool driverIsAnchor) const;

    /// Reads the partnerNullFillPass flag from the trigger buffer (set by the handler's termination
    /// pass). The outer probe branches on it to choose the partner-anchored null-fill pass.
    [[nodiscard]] nautilus::val<bool> isPartnerNullFillPass(RecordBuffer& recordBuffer) const;

    /// Interval predicate: anchorTs + lowerBound <= partnerTs <= anchorTs + upperBound. `anchorRecord`
    /// is an anchor-side (left input) tuple, `partnerRecord` a partner-side (right input) tuple.
    [[nodiscard]] nautilus::val<bool>
    intervalPredicateHolds(ExecutionContext& executionCtx, Record& anchorRecord, Record& partnerRecord) const;

    std::unique_ptr<TimeFunction> anchorTimeFunction;
    std::unique_ptr<TimeFunction> partnerTimeFunction;
    IntervalBound lowerBound;
    IntervalBound upperBound;
    std::shared_ptr<PagedVectorTupleLayout> anchorTupleLayout;
    std::shared_ptr<PagedVectorTupleLayout> partnerTupleLayout;
    std::vector<Record::RecordFieldIdentifier> anchorKeyFieldNames;
    std::vector<Record::RecordFieldIdentifier> partnerKeyFieldNames;
    bool emitAnchorNullFill;
    bool emitPartnerNullFill;
};

}
