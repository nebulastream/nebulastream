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

#include <Join/StreamJoinOperatorHandler.hpp>

#include <map>
#include <memory>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{
StreamJoinOperatorHandler::StreamJoinOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    JoinTriggerStrategy triggerStrategy)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore)), triggerStrategy(std::move(triggerStrategy))
{
}

void StreamJoinOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    const EmitSlicesFn emitFn
        = [this](
              const std::vector<std::shared_ptr<Slice>>& leftSlices,
              const std::vector<std::shared_ptr<Slice>>& rightSlices,
              ProbeTaskType probeTaskType,
              const WindowInfo& windowInfo,
              const SequenceData& sequenceData,
              PipelineExecutionContext* ctx) { emitSlicesToProbe(leftSlices, rightSlices, probeTaskType, windowInfo, sequenceData, ctx); };

    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        std::visit([&](const auto& strategy) { strategy.triggerWindow(allSlices, windowInfo, emitFn, pipelineCtx); }, triggerStrategy);
    }
}

}
