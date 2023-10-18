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
#include "Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp"
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {
void StreamJoinOperatorHandler::start(PipelineExecutionContextPtr, uint32_t) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}
void StreamJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

std::optional<StreamSlicePtr> StreamJoinOperatorHandler::getSliceBySliceIdentifier(uint64_t sliceIdentifier) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

std::optional<StreamSlicePtr> StreamJoinOperatorHandler::getSliceBySliceIdentifier(const RLockedSlices& slicesLocked,
                                                                                   uint64_t sliceIdentifier) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

std::optional<StreamSlicePtr> StreamJoinOperatorHandler::getSliceBySliceIdentifier(const WLockedSlices& slicesLocked,
                                                                                   uint64_t sliceIdentifier) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

void StreamJoinOperatorHandler::triggerAllSlices(PipelineExecutionContext* pipelineCtx) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

void StreamJoinOperatorHandler::checkAndTriggerWindows(const BufferMetaData& bufferMetaData,
                                                       PipelineExecutionContext* pipelineCtx) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

void StreamJoinOperatorHandler::deleteSlices(const BufferMetaData& bufferMetaData) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

uint64_t StreamJoinOperatorHandler::getNumberOfSlices() { NES_THROW_RUNTIME_ERROR("Not Implemented!"); }

uint64_t StreamJoinOperatorHandler::getNumberOfTuplesInSlice(uint64_t sliceIdentifier,
                                                             QueryCompilation::JoinBuildSideType buildSide) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

OriginId StreamJoinOperatorHandler::getOutputOriginId() const { NES_THROW_RUNTIME_ERROR("Not Implemented!"); }

uint64_t StreamJoinOperatorHandler::getNextSequenceNumber() { NES_THROW_RUNTIME_ERROR("Not Implemented!"); }

void StreamJoinOperatorHandler::setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

void StreamJoinOperatorHandler::updateWatermarkForWorker(uint64_t watermark, uint64_t workerId) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

uint64_t StreamJoinOperatorHandler::getMinWatermarkForWorker() { NES_THROW_RUNTIME_ERROR("Not Implemented!"); }

uint64_t StreamJoinOperatorHandler::getWindowSlide() const { return windowSlide; }

uint64_t StreamJoinOperatorHandler::getWindowSize() const { return windowSize; }

StreamJoinOperatorHandler::StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                                     const OriginId outputOriginId,
                                                     const uint64_t windowSize,
                                                     const uint64_t windowSlide,
                                                     uint64_t sizeOfRecordLeft,
                                                     uint64_t sizeOfRecordRight)
    : numberOfWorkerThreads(1), sliceAssigner(windowSize, windowSlide), windowSize(windowSize), windowSlide(windowSlide),
      watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins)),
      watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector<OriginId>(1, outputOriginId))),
      outputOriginId(outputOriginId), sequenceNumber(1), sizeOfRecordLeft(sizeOfRecordLeft),
      sizeOfRecordRight(sizeOfRecordRight) {}

}// namespace NES::Runtime::Execution::Operators