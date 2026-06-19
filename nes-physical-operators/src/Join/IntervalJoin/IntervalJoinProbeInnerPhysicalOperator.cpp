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

#include <Join/IntervalJoin/IntervalJoinProbeInnerPhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
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

IntervalJoinProbeInnerPhysicalOperator::IntervalJoinProbeInnerPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    const JoinSchema& joinSchema,
    std::unique_ptr<TimeFunction> anchorTimeFunction,
    std::unique_ptr<TimeFunction> partnerTimeFunction,
    const std::int64_t lowerBound,
    const std::int64_t upperBound,
    std::shared_ptr<TupleBufferRef> anchorMemoryProvider,
    std::shared_ptr<TupleBufferRef> partnerMemoryProvider,
    std::vector<Record::RecordFieldIdentifier> anchorKeyFieldNames,
    std::vector<Record::RecordFieldIdentifier> partnerKeyFieldNames)
    : IntervalJoinProbePhysicalOperator(
          operatorHandlerId,
          std::move(joinFunction),
          std::move(windowMetaData),
          joinSchema,
          std::move(anchorTimeFunction),
          std::move(partnerTimeFunction),
          lowerBound,
          upperBound,
          std::move(anchorMemoryProvider),
          std::move(partnerMemoryProvider),
          std::move(anchorKeyFieldNames),
          std::move(partnerKeyFieldNames),
          /*emitAnchorNullFill=*/false)
{
}

void IntervalJoinProbeInnerPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    // todo if there is so little difference between inner and outer join then merge it into one class with separate methods. maybe even we can set the specific runJoinPass to call during the lowering. this should be possible
    prepareOpen(executionCtx, recordBuffer);
    runJoinPass(executionCtx, recordBuffer, /*driverIsAnchor=*/true);
}

}
