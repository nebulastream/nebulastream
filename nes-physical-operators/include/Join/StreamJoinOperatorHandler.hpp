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

#include <map>
#include <memory>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/JoinTriggerStrategy.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// All possible trigger strategies, selected at lowering time based on the join type.
/// std::variant avoids virtual dispatch and heap allocation — the strategy is stored inline.
using JoinTriggerStrategy = std::variant<
    InnerJoinTriggerStrategy,
    OuterJoinTriggerStrategy<true, false>,
    OuterJoinTriggerStrategy<false, true>,
    OuterJoinTriggerStrategy<true, true>>;

/// This operator is the general join operator handler. It is expected that all StreamJoinOperatorHandlers inherit from this class.
/// It delegates window triggering to a JoinTriggerStrategy configured at lowering time and delegates the actual probe
/// emission to the specific join implementation via emitSlicesToProbe().
class StreamJoinOperatorHandler : public WindowBasedOperatorHandler
{
public:
    StreamJoinOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        JoinTriggerStrategy triggerStrategy);

protected:
    /// Delegates to the configured JoinTriggerStrategy for each triggered window.
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;

    /// Emits probe buffers for a given set of left and right slices with the specified task type.
    /// Each join implementation (HJ, NLJ, ...) packs its own data format into the probe buffer.
    virtual void emitSlicesToProbe(
        const std::vector<std::shared_ptr<Slice>>& leftSlices,
        const std::vector<std::shared_ptr<Slice>>& rightSlices,
        ProbeTaskType probeTaskType,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        PipelineExecutionContext* pipelineCtx)
        = 0;

    JoinTriggerStrategy triggerStrategy;
};
}
