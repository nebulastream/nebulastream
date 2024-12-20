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
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Execution/Operators/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/WindowBasedOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Execution.hpp>
#include <Util/SliceCache/SliceCache.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators
{

WindowBasedOperatorHandler::WindowBasedOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    SliceCachePtr sliceCache,
    bool globalSliceCache)
    : sliceAndWindowStore(std::move(sliceAndWindowStore))
    , globalSliceCache(globalSliceCache)
    , watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins))
    , watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector(1, outputOriginId)))
    , numberOfWorkerThreads(1)
    , outputOriginId(outputOriginId)
{
    sliceCaches.push_back(sliceCache);
}

void WindowBasedOperatorHandler::setWorkerThreads(const uint64_t numberOfWorkerThreads)
{
    WindowBasedOperatorHandler::numberOfWorkerThreads = numberOfWorkerThreads;
    // Create slice cache for each worker thread
    for (uint64_t i = 0; i < numberOfWorkerThreads - 1; ++i)
    {
        if (globalSliceCache)
        {
            // use the same shared_ptr for a global slice cache
            sliceCaches.push_back(sliceCaches[0]);
        }
        else
        {
            // clone the slice cache for local slice caches
            sliceCaches.push_back(sliceCaches[0]->clone());
        }
    }
}

void WindowBasedOperatorHandler::setBufferProvider(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
{
    WindowBasedOperatorHandler::bufferProvider = std::move(bufferProvider);
}


void WindowBasedOperatorHandler::start(PipelineExecutionContext& pipelineExecutionContext, uint32_t)
{
    numberOfWorkerThreads = pipelineExecutionContext.getNumberOfWorkerThreads();
    bufferProvider = pipelineExecutionContext.getBufferManager();
}

void WindowBasedOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

WindowSlicesStoreInterface& WindowBasedOperatorHandler::getSliceAndWindowStore() const
{
    return *sliceAndWindowStore;
}

SliceCachePtr WindowBasedOperatorHandler::getSliceCacheForWorker(WorkerThreadId workerThreadId) const
{
    return sliceCaches[workerThreadId % numberOfWorkerThreads];
}

void WindowBasedOperatorHandler::garbageCollectSlicesAndWindows(const BufferMetaData& bufferMetaData) const
{
    const auto newGlobalWaterMarkProbe
        = watermarkProcessorProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    std::vector<SliceEnd> slicesToDelete = sliceAndWindowStore->garbageCollectSlicesAndWindows(newGlobalWaterMarkProbe);
    for (const auto& sliceCache : sliceCaches)
    {
        sliceCache->deleteSlicesFromCache(slicesToDelete);
    }
}
}
