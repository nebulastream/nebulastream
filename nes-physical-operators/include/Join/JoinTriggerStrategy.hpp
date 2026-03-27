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
#include <SliceStore/WindowSlicesStoreInterface.hpp>

namespace NES
{

/// Trigger strategy for inner joins and cartesian products.
/// Emits NxN MATCH_PAIRS tasks: each left slice paired with each right slice.
struct InnerJoinTriggerStrategy
{
    static void triggerWindow(
        const std::vector<std::shared_ptr<Slice>>& allSlices,
        const WindowInfoAndSequenceNumber& windowInfo,
        const EmitSlicesFn& emitFn,
        PipelineExecutionContext* pipelineCtx);
};

/// Trigger strategy for outer joins (left, right, full).
/// Emits NxN MATCH_PAIRS tasks plus null-fill tasks for the configured sides.
/// Template parameters eliminate runtime branching via if constexpr.
/// - Left outer:  OuterJoinTriggerStrategy<true, false>
/// - Right outer: OuterJoinTriggerStrategy<false, true>
/// - Full outer:  OuterJoinTriggerStrategy<true, true>
template <bool EmitLeftNullFill, bool EmitRightNullFill>
struct OuterJoinTriggerStrategy
{
    static void triggerWindow(
        const std::vector<std::shared_ptr<Slice>>& allSlices,
        const WindowInfoAndSequenceNumber& windowInfo,
        const EmitSlicesFn& emitFn,
        PipelineExecutionContext* pipelineCtx);
};

}
