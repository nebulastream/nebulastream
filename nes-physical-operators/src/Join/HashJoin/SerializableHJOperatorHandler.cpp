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

#include <Join/HashJoin/SerializableHJOperatorHandler.hpp>
#include <Join/HashJoin/SerializableHJSlice.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Nautilus/State/Conversion/JoinStateConverters.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <Time/Timestamp.hpp>
#include <Sequencing/SequenceData.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES {

SerializableHJOperatorHandler::SerializableHJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
{
    state_.metadata.operatorType = 2; // join
    state_.metadata.version = 1;
    // default config; will be set by rewrite
    state_.config.keySize = 8;
    state_.config.valueSize = 8;
    state_.config.numberOfBuckets = 1024;
    state_.config.pageSize = 4096;
}

SerializableHJOperatorHandler::SerializableHJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const State::JoinState& initialState)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
    , state_(initialState)
{
}

std::shared_ptr<SerializableHJOperatorHandler> SerializableHJOperatorHandler::deserialize(
    const TupleBuffer& buffer,
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
{
    auto st = NES::Serialization::StateSerializer<State::JoinState>::deserialize(buffer);
    if (!st) {
        throw std::runtime_error("Failed to deserialize JoinState");
    }
    auto handler = std::make_shared<SerializableHJOperatorHandler>(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), st.value());
    handler->rehydrateFromState();
    return handler;
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
SerializableHJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    const auto& args = dynamic_cast<const CreateNewHashMapSliceArgs&>(newSlicesArguments);
    auto keySize = state_.config.keySize;
    auto valueSize = state_.config.valueSize;
    auto buckets = state_.config.numberOfBuckets;

    auto workerThreads = this->numberOfWorkerThreads == 0 ? 1 : this->numberOfWorkerThreads;
    return std::function([
        copyArgs = args,
        numberOfWorkerThreads = workerThreads,
        keySize,
        valueSize,
        buckets
    ](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>> {
        return {std::make_shared<SerializableHJSlice>(sliceStart, sliceEnd, copyArgs, numberOfWorkerThreads, keySize, valueSize, buckets)};
    });
}

void SerializableHJOperatorHandler::captureState()
{
    // Query slice store first; if no observable slices, preserve existing state (recovery/round-trip scenarios)
    auto windowsToSlices = sliceAndWindowStore->getAllNonTriggeredSlices();
    if (windowsToSlices.empty()) {
        if (watermarkProcessorBuild) {
            state_.metadata.lastWatermark = watermarkProcessorBuild->getCurrentWatermark().getRawValue();
        }
        return;
    }

    state_.hashMaps.clear();
    state_.windows.clear();

    const uint64_t workerCount = numberOfWorkerThreads ? numberOfWorkerThreads : 1;
    uint64_t totalTuples = 0;
    for (const auto& [winInfoSeq, slices] : windowsToSlices)
    {
        const auto startIndex = static_cast<uint64_t>(state_.hashMaps.size());
        uint64_t countForWindow = 0;

        for (const auto& slice : slices)
        {
            auto serSlice = std::dynamic_pointer_cast<SerializableHJSlice>(slice);
            if (!serSlice) { continue; }

            for (uint64_t tid = 0; tid < workerCount; ++tid)
            {
                // Left
                const auto& lstate = serSlice->getHashMapState(WorkerThreadId(tid), JoinBuildSideType::Left);
                auto leftSt = NES::State::toState(lstate);
                totalTuples += leftSt.tupleCount;
                state_.hashMaps.emplace_back(std::move(leftSt));
                ++countForWindow;
                // Right
                const auto& rstate = serSlice->getHashMapState(WorkerThreadId(tid), JoinBuildSideType::Right);
                auto rightSt = NES::State::toState(rstate);
                totalTuples += rightSt.tupleCount;
                state_.hashMaps.emplace_back(std::move(rightSt));
                ++countForWindow;
            }
        }

        State::JoinState::Window w{};
        w.start = winInfoSeq.windowInfo.windowStart.getRawValue();
        w.end = winInfoSeq.windowInfo.windowEnd.getRawValue();
        w.state = 0;
        w.firstHashMapIndex = startIndex;
        w.hashMapCount = countForWindow;
        state_.windows.emplace_back(std::move(w));
    }

    state_.metadata.processedRecords = totalTuples;
    if (watermarkProcessorBuild) {
        state_.metadata.lastWatermark = watermarkProcessorBuild->getCurrentWatermark().getRawValue();
    }
}

void SerializableHJOperatorHandler::rehydrateFromState()
{
    if (numberOfWorkerThreads == 0) {
        for (const auto& w : state_.windows) {
            if (w.hashMapCount >= 2) { numberOfWorkerThreads = w.hashMapCount / 2; break; }
        }
        if (numberOfWorkerThreads == 0) { numberOfWorkerThreads = 1; }
    }

    std::vector<std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec>> noops(2);
    CreateNewHashMapSliceArgs args{
        std::move(noops),
        state_.config.keySize,
        state_.config.valueSize,
        state_.config.pageSize,
        state_.config.numberOfBuckets};
    auto makeSlice = getCreateNewSlicesFunction(args);

    for (const auto& w : state_.windows) {
        auto slices = sliceAndWindowStore->getSlicesOrCreate(Timestamp(w.start), makeSlice);
        if (slices.empty()) { continue; }
        auto serSlice = std::dynamic_pointer_cast<SerializableHJSlice>(slices[0]);
        if (!serSlice) { continue; }

        const uint64_t base = w.firstHashMapIndex;
        const uint64_t n = std::min<uint64_t>(numberOfWorkerThreads, w.hashMapCount / 2);
        for (uint64_t i = 0; i < n; ++i) {
            auto& leftTarget = serSlice->getHashMapState(WorkerThreadId(i), JoinBuildSideType::Left);
            auto& rightTarget = serSlice->getHashMapState(WorkerThreadId(i), JoinBuildSideType::Right);
            leftTarget = NES::State::fromState(state_.hashMaps[base + (2 * i + 0)]);
            rightTarget = NES::State::fromState(state_.hashMaps[base + (2 * i + 1)]);
        }
    }
}

void SerializableHJOperatorHandler::emitSlicesToProbe(
    Slice& sliceLeft,
    Slice& sliceRight,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    uint64_t totalNumberOfTuples = 0;

    auto getHashMapsForSlice = [&](const Slice& slice, const JoinBuildSideType& buildSide)
    {
        std::vector<Nautilus::Interface::HashMap*> allHashMaps;
        const auto* const serSlice = dynamic_cast<const SerializableHJSlice*>(&slice);
        INVARIANT(serSlice != nullptr, "Slice must be of type SerializableHJSlice!");
        for (uint64_t hashMapIdx = 0; hashMapIdx < serSlice->getNumberOfHashMapsForSide(); ++hashMapIdx)
        {
            if (auto* hashMap = serSlice->getHashMapPtr(WorkerThreadId(hashMapIdx), buildSide);
                hashMap and hashMap->getNumberOfTuples() > 0)
            {
                allHashMaps.emplace_back(hashMap);
                totalNumberOfTuples += hashMap->getNumberOfTuples();
            }
        }
        return allHashMaps;
    };

    const auto leftHashMaps = getHashMapsForSlice(sliceLeft, JoinBuildSideType::Left);
    const auto rightHashMaps = getHashMapsForSlice(sliceRight, JoinBuildSideType::Right);

    const auto neededBufferSize
        = sizeof(EmittedHJWindowTrigger) + ((leftHashMaps.size() + rightHashMaps.size()) * sizeof(Nautilus::Interface::HashMap*));
    const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not tupleBufferVal.has_value())
    {
        throw CannotAllocateBuffer("{}B for the hash join window trigger were requested", neededBufferSize);
    }

    auto tupleBuffer = tupleBufferVal.value();
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(totalNumberOfTuples);

    auto* bufferMemory = tupleBuffer.getBuffer<EmittedHJWindowTrigger>();
    bufferMemory->windowInfo = windowInfo;
    bufferMemory->leftNumberOfHashMaps = leftHashMaps.size();
    bufferMemory->rightNumberOfHashMaps = rightHashMaps.size();

    const auto leftHashMapPtrSizeInByte = leftHashMaps.size() * sizeof(Nautilus::Interface::HashMap*);
    auto* addressFirstLeftHashMapPtr = std::bit_cast<int8_t*>(bufferMemory) + sizeof(EmittedHJWindowTrigger);
    auto* addressFirstRightHashMapPtr = std::bit_cast<int8_t*>(bufferMemory) + sizeof(EmittedHJWindowTrigger) + leftHashMapPtrSizeInByte;
    bufferMemory->leftHashMaps = std::bit_cast<Nautilus::Interface::HashMap**>(addressFirstLeftHashMapPtr);
    bufferMemory->rightHashMaps = std::bit_cast<Nautilus::Interface::HashMap**>(addressFirstRightHashMapPtr);
    std::ranges::copy(leftHashMaps, std::bit_cast<Nautilus::Interface::HashMap**>(addressFirstLeftHashMapPtr));
    std::ranges::copy(rightHashMaps, std::bit_cast<Nautilus::Interface::HashMap**>(addressFirstRightHashMapPtr));

    pipelineCtx->emitBuffer(tupleBuffer);
}

} // namespace NES
