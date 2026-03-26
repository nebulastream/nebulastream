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
#include <ranges>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{
StreamJoinOperatorHandler::StreamJoinOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
{
}

void StreamJoinOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    /// For every window, we have to trigger all combination of slices. This is necessary, as we have to give the probe operator all
    /// combinations of slices for a given window to ensure that it has seen all tuples of the window.
    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        auto range = windowInfo.sequenceRange;
        auto subranges = range.fracture(allSlices.size() * allSlices.size());
        size_t subrangeIdx = 0;
        for (const auto& sliceLeft : allSlices)
        {
            for (const auto& sliceRight : allSlices)
            {
                emitSlicesToProbe(*sliceLeft, *sliceRight, windowInfo.windowInfo, subranges.at(subrangeIdx++), pipelineCtx);
            }
        }
    }
}


}
