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

#include <algorithm>
#include <ranges>
#include <utility>
#include <Execution/Operators/WindowBasedOperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators
{

WindowBasedOperatorHandler::WindowBasedOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
    : sliceAndWindowStore(std::move(sliceAndWindowStore))
    , watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins))
    , watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector(1, outputOriginId)))
    , numberOfWorkerThreads(1)
    , outputOriginId(outputOriginId)
{
}

void WindowBasedOperatorHandler::setWorkerThreads(const uint64_t numberOfWorkerThreads)
{
    WindowBasedOperatorHandler::numberOfWorkerThreads = numberOfWorkerThreads;
}

void WindowBasedOperatorHandler::setBufferProvider(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
{
    WindowBasedOperatorHandler::bufferProvider = std::move(bufferProvider);
}


void WindowBasedOperatorHandler::start(PipelineExecutionContextPtr pipelineExecutionContext, uint32_t)
{
    numberOfWorkerThreads = pipelineExecutionContext->getNumberOfWorkerThreads();
    bufferProvider = pipelineExecutionContext->getBufferManager();
}

void WindowBasedOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr)
{
}

WindowSlicesStoreInterface& WindowBasedOperatorHandler::getSliceAndWindowStore() const
{
    return *sliceAndWindowStore;
}


void WindowBasedOperatorHandler::garbageCollectSlicesAndWindows(const BufferMetaData& bufferMetaData) const
{
    const auto newGlobalWaterMarkProbe
        = watermarkProcessorProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    sliceAndWindowStore->garbageCollectSlicesAndWindows(newGlobalWaterMarkProbe);
}
}
