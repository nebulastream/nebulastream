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
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>

namespace NES
{

/// Window-trigger strategy for stream joins, configured at lowering time from the join type. Always
/// emits NxN MATCH_PAIRS. Outer joins additionally emit per-side NULL_FILL tasks.
/// - Inner / cartesian: both flags false
/// - Left outer:  {emitLeftNullFill = true}
/// - Right outer: {emitRightNullFill = true}
/// - Full outer:  {emitLeftNullFill = true, emitRightNullFill = true}
struct JoinTriggerStrategy
{
    bool emitLeftNullFill = false;
    bool emitRightNullFill = false;

    void triggerWindow(
        const std::vector<std::shared_ptr<Slice>>& allSlices,
        const WindowInfoAndSequenceNumber& windowInfo,
        const EmitSlicesFn& emitFn,
        PipelineExecutionContext* pipelineCtx) const;
};

}
