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
#include <Join/IntervalJoin/IntervalJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Outer interval-join probe (LEFT/RIGHT/FULL). The anchor-driven pass null-fills unmatched anchor
/// tuples (LEFT/FULL, controlled by emitAnchorNullFill); a partner-anchored null-fill pass, triggered
/// by the handler at termination, null-fills unmatched partner tuples (RIGHT/FULL).
class IntervalJoinProbeOuterPhysicalOperator final : public IntervalJoinProbePhysicalOperator
{
public:
    IntervalJoinProbeOuterPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::unique_ptr<TimeFunction> anchorTimeFunction,
        std::unique_ptr<TimeFunction> partnerTimeFunction,
        std::int64_t lowerBound,
        std::int64_t upperBound,
        std::shared_ptr<TupleBufferRef> anchorMemoryProvider,
        std::shared_ptr<TupleBufferRef> partnerMemoryProvider,
        std::vector<Record::RecordFieldIdentifier> anchorKeyFieldNames,
        std::vector<Record::RecordFieldIdentifier> partnerKeyFieldNames,
        bool emitAnchorNullFill);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
};

}
