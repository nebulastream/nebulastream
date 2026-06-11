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
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Watermark/TimeFunction.hpp>
#include <Windowing/WindowMetaData.hpp>

namespace NES
{

/// Probe-side physical operator for the streaming interval join.
///
/// Per buffer (each buffer represents one anchor with up to 3 partner slice
/// ends), this operator:
///   1. Decodes the EmittedIntervalJoinWindowTrigger.
///   2. Resolves the anchor (left) slice and each partner (right) slice.
///   3. Reads tuples from the post-combine PagedVectors (slot 0).
///   4. Cross-joins left * partner, filtering by:
///        a) the interval predicate: rightTs in [leftTs + lowerBound, leftTs + upperBound]
///        b) the user join expression (e.g. id = id).
///
/// Inherits StreamJoinProbePhysicalOperator for `createJoinedRecord`, but
/// overrides `close` because the inherited WindowProbePhysicalOperator::close
/// would dynamic_cast the handler to WindowBasedOperatorHandler, which our
/// handler does not extend.
class IntervalJoinProbePhysicalOperator final : public StreamJoinProbePhysicalOperator
{
public:
    IntervalJoinProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::unique_ptr<TimeFunction> leftTimeFunction,
        std::unique_ptr<TimeFunction> rightTimeFunction,
        std::int64_t lowerBound,
        std::int64_t upperBound,
        std::shared_ptr<TupleBufferRef> leftMemoryProvider,
        std::shared_ptr<TupleBufferRef> rightMemoryProvider,
        std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames,
        std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames);

    IntervalJoinProbePhysicalOperator(const IntervalJoinProbePhysicalOperator& other);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

private:
    std::unique_ptr<TimeFunction> leftTimeFunction;
    std::unique_ptr<TimeFunction> rightTimeFunction;
    std::int64_t lowerBound;
    std::int64_t upperBound;
    std::shared_ptr<TupleBufferRef> leftMemoryProvider;
    std::shared_ptr<TupleBufferRef> rightMemoryProvider;
    std::vector<Record::RecordFieldIdentifier> leftKeyFieldNames;
    std::vector<Record::RecordFieldIdentifier> rightKeyFieldNames;
};

}
