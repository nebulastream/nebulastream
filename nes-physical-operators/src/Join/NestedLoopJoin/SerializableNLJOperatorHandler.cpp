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

#include <Join/NestedLoopJoin/SerializableNLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Nautilus/State/Conversion/JoinStateConverters.hpp>
#include <Time/Timestamp.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES {

SerializableNLJOperatorHandler::SerializableNLJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
{
    state_.metadata.operatorType = 2; // join
    state_.metadata.version = 1;
    state_.config.pageSize = 4096;
}

SerializableNLJOperatorHandler::SerializableNLJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const State::JoinState& initialState)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
    , state_(initialState)
{
}

std::shared_ptr<SerializableNLJOperatorHandler> SerializableNLJOperatorHandler::deserialize(
    const TupleBuffer& buffer,
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
{
    auto st = NES::Serialization::StateSerializer<State::JoinState>::deserialize(buffer);
    if (!st) { throw std::runtime_error("Failed to deserialize JoinState"); }
    auto handler = std::make_shared<SerializableNLJOperatorHandler>(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), st.value());
    handler->rehydrateFromState();
    return handler;
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
SerializableNLJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments&) const
{
    auto workerThreads = this->numberOfWorkerThreads == 0 ? 1 : this->numberOfWorkerThreads;
    return std::function([
        numberOfWorkerThreads = workerThreads
    ](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>> {
        return {std::make_shared<NLJSlice>(sliceStart, sliceEnd, numberOfWorkerThreads)};
    });
}

void SerializableNLJOperatorHandler::captureState()
{
    // If slice store has nothing observable (e.g., test/manual state injection), keep existing state
    auto windowsToSlices = sliceAndWindowStore->getAllNonTriggeredSlices();
    if (windowsToSlices.empty()) {
        if (watermarkProcessorBuild) {
            state_.metadata.lastWatermark = watermarkProcessorBuild->getCurrentWatermark().getRawValue();
        }
        return;
    }

    state_.leftVectors.clear();
    state_.rightVectors.clear();
    state_.windows.clear();

    const uint64_t workerCount = numberOfWorkerThreads ? numberOfWorkerThreads : 1;
    uint64_t totalEntries = 0;
    for (const auto& [winInfoSeq, slices] : windowsToSlices)
    {
        const auto leftBase = static_cast<uint64_t>(state_.leftVectors.size());
        const auto rightBase = static_cast<uint64_t>(state_.rightVectors.size());
        uint64_t leftCount = 0;
        uint64_t rightCount = 0;

        for (const auto& slice : slices)
        {
            auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slice);
            if (!nljSlice) { continue; }

            for (uint64_t tid = 0; tid < workerCount; ++tid)
            {
                auto* lvec = nljSlice->getPagedVectorRefLeft(WorkerThreadId(tid));
                auto* rvec = nljSlice->getPagedVectorRefRight(WorkerThreadId(tid));
                if (lvec) {
                    state_.leftVectors.emplace_back(lvec->extractState());
                    totalEntries += lvec->getTotalNumberOfEntries();
                    ++leftCount;
                }
                if (rvec) {
                    state_.rightVectors.emplace_back(rvec->extractState());
                    totalEntries += rvec->getTotalNumberOfEntries();
                    ++rightCount;
                }
            }
        }

        State::JoinState::Window w{};
        w.start = winInfoSeq.windowInfo.windowStart.getRawValue();
        w.end = winInfoSeq.windowInfo.windowEnd.getRawValue();
        w.state = 0;
        w.firstLeftVectorIndex = leftBase;
        w.leftVectorCount = leftCount;
        w.firstRightVectorIndex = rightBase;
        w.rightVectorCount = rightCount;
        state_.windows.emplace_back(std::move(w));
    }

    state_.metadata.processedRecords = totalEntries;
    if (watermarkProcessorBuild) {
        state_.metadata.lastWatermark = watermarkProcessorBuild->getCurrentWatermark().getRawValue();
    }
}

void SerializableNLJOperatorHandler::rehydrateFromState()
{
    if (numberOfWorkerThreads == 0) {
        for (const auto& w : state_.windows) {
            if (w.leftVectorCount > 0) { numberOfWorkerThreads = w.leftVectorCount; break; }
            if (w.rightVectorCount > 0) { numberOfWorkerThreads = w.rightVectorCount; break; }
        }
        if (numberOfWorkerThreads == 0) { numberOfWorkerThreads = 1; }
    }

    CreateNewSlicesArguments args{};
    auto makeSlice = getCreateNewSlicesFunction(args);

    for (const auto& w : state_.windows) {
        auto slices = sliceAndWindowStore->getSlicesOrCreate(Timestamp(w.start), makeSlice);
        if (slices.empty()) { continue; }
        auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slices[0]);
        if (!nljSlice) { continue; }

        const uint64_t ln = std::min<uint64_t>(w.leftVectorCount, numberOfWorkerThreads);
        const uint64_t rn = std::min<uint64_t>(w.rightVectorCount, numberOfWorkerThreads);
        for (uint64_t i = 0; i < ln; ++i) {
            auto* vec = nljSlice->getPagedVectorRefLeft(WorkerThreadId(i));
            vec->extractState() = state_.leftVectors[w.firstLeftVectorIndex + i];
        }
        for (uint64_t i = 0; i < rn; ++i) {
            auto* vec = nljSlice->getPagedVectorRefRight(WorkerThreadId(i));
            vec->extractState() = state_.rightVectors[w.firstRightVectorIndex + i];
        }
    }
}

void SerializableNLJOperatorHandler::emitSlicesToProbe(
    Slice& sliceLeft,
    Slice& sliceRight,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    auto& nljSliceLeft = dynamic_cast<NLJSlice&>(sliceLeft);
    auto& nljSliceRight = dynamic_cast<NLJSlice&>(sliceRight);
    nljSliceLeft.combinePagedVectors();
    nljSliceRight.combinePagedVectors();
    const auto totalNumberOfTuples = nljSliceLeft.getNumberOfTuplesLeft() + nljSliceRight.getNumberOfTuplesRight();
    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
    
    auto* bufferMemory = tupleBuffer.getBuffer<EmittedNLJWindowTrigger>();
    bufferMemory->leftSliceEnd = sliceLeft.getSliceEnd();
    bufferMemory->rightSliceEnd = sliceRight.getSliceEnd();
    bufferMemory->windowInfo = windowInfo;
    
    pipelineCtx->emitBuffer(tupleBuffer);
}

}
