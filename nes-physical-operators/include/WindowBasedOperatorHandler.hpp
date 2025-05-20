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
#include <functional>
#include <map>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
/// This is the base class for all window-based operator handlers, e.g., join and aggregation.
/// It assumes that they have a build and a probe phase.
/// The build phase is the phase where the operator adds tuples to window(s) / the state.
/// The probe phase gets triggered by the build phase and is the phase where the operator processes the build-up state, e.g., performs the join or aggregation.
class WindowBasedOperatorHandler : public OperatorHandler
{
public:
    WindowBasedOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore);

    ~WindowBasedOperatorHandler() override = default;

    /// We can not call opHandler->start() from Nautilus, as we only get a pointer in the proxy function in Nautilus, e.g., setupProxy() in StreamJoinBuild
    void setWorkerThreads(uint64_t numberOfWorkerThreads);
    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType queryTerminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    WindowSlicesStoreInterface& getSliceAndWindowStore() const;

    /// Updates the corresponding watermark processor, and then garbage collects all slices and windows that are not valid anymore
    void garbageCollectSlicesAndWindows(const BufferMetaData& bufferMetaData) const;

    /// Checks and triggers windows that are ready to be triggered, e.g., the watermark has passed the window end for time-based windows.
    /// This method updates the watermarkProcessor and is thread-safe
    virtual void checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);

    /// Triggers all windows that have not been already emitted to the probe
    virtual void triggerAllWindows(PipelineExecutionContext* pipelineCtx);

    /// Gives the specific operator handler the chance to provide a function that creates new slices
    /// This method is being called whenever a new slice is needed, e.g., receiving a timestamp that is not yet in the slice store.
    [[nodiscard]] virtual std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)> getCreateNewSlicesFunction() const = 0;

protected:
    /// Gets called if slices should be triggered once a window is ready to be emitted.
    /// Each window operator can be specific about what to do if the given slices are ready to be emitted
    virtual void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx)
        = 0;

    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorBuild;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorProbe;
    uint64_t numberOfWorkerThreads;
    const OriginId outputOriginId;
};
}
