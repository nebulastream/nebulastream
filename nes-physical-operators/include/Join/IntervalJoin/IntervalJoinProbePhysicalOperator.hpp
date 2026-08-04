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

#include <memory>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/IntervalBound.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <val_bool.hpp>

namespace NES
{

/// Probe operator for the streaming interval join (inner only). A probe buffer is one anchor slice with up
/// to 3 partner slice ends; the probe emits matched (anchor, partner) pairs.
/// The per-buffer close is overridden because the base close path dynamic_casts the handler to
/// WindowBasedOperatorHandler, which the interval-join handler does not extend.
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
        std::vector<Record::RecordFieldIdentifier> partnerKeyFieldNames);

    IntervalJoinProbePhysicalOperator(const IntervalJoinProbePhysicalOperator& other);
    IntervalJoinProbePhysicalOperator(IntervalJoinProbePhysicalOperator&& other) noexcept = default;
    /// Assignment is deleted: the base StreamJoinProbePhysicalOperator is not assignable. The framework
    /// only needs this operator copy-constructible (for cloning), so constructors suffice.
    IntervalJoinProbePhysicalOperator& operator=(const IntervalJoinProbePhysicalOperator& other) = delete;
    IntervalJoinProbePhysicalOperator& operator=(IntervalJoinProbePhysicalOperator&& other) = delete;
    ~IntervalJoinProbePhysicalOperator() override = default;

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

protected:
    /// Propagate buffer metadata onto the execution context and open the per-side time functions.
    void prepareOpen(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// Scans the anchor slice against its partner slices, emitting each matched (anchor, partner) pair.
    void runJoinPass(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// anchorTs + lowerBound <= partnerTs <= anchorTs + upperBound.
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
};

}
