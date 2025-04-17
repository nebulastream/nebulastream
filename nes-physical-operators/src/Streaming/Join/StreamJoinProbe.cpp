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

#include <cstdint>
#include <memory>
#include <vector>

#include <utility>
#include <Execution/Functions/PhysicalFunction.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinProbe.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/WindowOperatorProbe.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Operators/Streaming/Join/StreamJoinProbe.hpp>
#include <Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <static.hpp>

namespace NES
{

StreamJoinProbe::StreamJoinProbe(
    const uint64_t operatorHandlerIndex,
    const std::shared_ptr<Functions::Function>& joinFunction,
    WindowMetaData windowMetaData,
    JoinSchema joinSchema)
    : WindowOperatorProbe(operatorHandlerIndex, std::move(windowMetaData)), joinFunction(joinFunction), joinSchema(std::move(joinSchema))
{
}

Record StreamJoinProbe::createJoinedRecord(
    const Record& leftRecord,
    const Record& rightRecord,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const std::vector<Record::RecordFieldIdentifier>& projectionsLeft,
    const std::vector<Record::RecordFieldIdentifier>& projectionsRight) const
{
    Record joinedRecord;

    /// Writing the window start, end, and key field
    joinedRecord.write(windowMetaData.windowStartFieldName, windowStart.convertToValue());
    joinedRecord.write(windowMetaData.windowEndFieldName, windowEnd.convertToValue());

    /// Writing the leftSchema fields, expect the join schema to have the fields in the same order then the left schema
    for (const auto& fieldName : nautilus::static_iterable(projectionsLeft))
    {
        joinedRecord.write(fieldName, leftRecord.read(fieldName));
    }

    /// Writing the rightSchema fields, expect the join schema to have the fields in the same order then the right schema
    for (const auto& fieldName : nautilus::static_iterable(projectionsRight))
    {
        joinedRecord.write(fieldName, rightRecord.read(fieldName));
    }

    return joinedRecord;
}

Record StreamJoinProbe::createJoinedRecord(
    const Record& leftRecord,
    const Record& rightRecord,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    return createJoinedRecord(
        leftRecord, rightRecord, windowStart, windowEnd, joinSchema.leftSchema->getFieldNames(), joinSchema.rightSchema->getFieldNames());
}
}
