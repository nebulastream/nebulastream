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
#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <WindowProbePhysicalOperator.hpp>
#include <static.hpp>

namespace NES
{

StreamJoinProbePhysicalOperator::StreamJoinProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId, PhysicalFunction joinFunction, WindowMetaData windowMetaData, JoinSchema joinSchema)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , joinFunction(std::move(joinFunction))
    , joinSchema(std::move(joinSchema))
{
}

Record StreamJoinProbePhysicalOperator::createJoinedRecord(
    const Record& leftRecord,
    const Record& rightRecord,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const std::vector<Record::RecordFieldIdentifier>& projectionsLeft,
    const std::vector<Record::RecordFieldIdentifier>& projectionsRight) const
{
    Record joinedRecord;

    /// Writing the window start, end, and key field
    joinedRecord.write(this->windowMetaData.windowStartFieldName, windowStart.convertToValue());
    joinedRecord.write(this->windowMetaData.windowEndFieldName, windowEnd.convertToValue());

    /// Writing the rightSchema fields, expect the join schema to have the fields in the same order then the right schema
    for (const auto& fieldName : nautilus::static_iterable(projectionsRight))
    {
        joinedRecord.write(fieldName, rightRecord.read(fieldName));
    }

    /// Writing the leftSchema fields, expect the join schema to have the fields in the same order then the left schema
    for (const auto& fieldName : nautilus::static_iterable(projectionsLeft))
    {
        joinedRecord.write(fieldName, leftRecord.read(fieldName));
    }

    return joinedRecord;
}

Record StreamJoinProbePhysicalOperator::createJoinedRecord(
    const Record& leftRecord,
    const Record& rightRecord,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    return createJoinedRecord(
        leftRecord, rightRecord, windowStart, windowEnd, joinSchema.leftSchema.getFieldNames(), joinSchema.rightSchema.getFieldNames());
}
}
