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
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <WindowProbePhysicalOperator.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// This class is the second phase of the stream join. The actual implementation (nested-loops, probing hash tables)
/// is not part of this class. This class takes care of the close() functionality as this universal.
class StreamJoinProbePhysicalOperator : public WindowProbePhysicalOperator
{
public:
    StreamJoinProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId, PhysicalFunction joinFunction, WindowMetaData windowMetaData, JoinSchema joinSchema);

protected:
    /// Creates a joined record out of the outer and inner record
    Record createJoinedRecord(
        const Record& outerRecord,
        const Record& innerRecord,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd,
        const std::vector<Record::RecordFieldIdentifier>& projectionsOuter,
        const std::vector<Record::RecordFieldIdentifier>& projectionsInner) const;

    PhysicalFunction joinFunction;
    JoinSchema joinSchema;
};
}
