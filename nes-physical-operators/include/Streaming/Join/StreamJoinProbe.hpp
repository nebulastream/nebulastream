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
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/Operator.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/WindowOperatorProbe.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// This class is the second phase of the stream join. The actual implementation (nested-loops, probing hash tables)
/// is not part of this class. This class takes care of the close() functionality as this universal.
/// Furthermore, it provides a method of creating the joined tuple
class StreamJoinProbe : public WindowOperatorProbe
{
public:
    StreamJoinProbe(
        uint64_t operatorHandlerIndex,
        const std::shared_ptr<Functions::Function>& joinFunction,
        WindowMetaData windowMetaData,
        JoinSchema joinSchema);

protected:
    /// Creates a joined record out of the left and right record, but it only uses the provided projection
    Record createJoinedRecord(
        const Record& leftRecord,
        const Record& rightRecord,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd,
        const std::vector<Record::RecordFieldIdentifier>& projectionsLeft,
        const std::vector<Record::RecordFieldIdentifier>& projectionsRight) const;

    /// Creates a joined record out of the left and right record
    Record createJoinedRecord(
        const Record& leftRecord,
        const Record& rightRecord,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;

    std::shared_ptr<Functions::Function> joinFunction;
    JoinSchema joinSchema;
};
}
