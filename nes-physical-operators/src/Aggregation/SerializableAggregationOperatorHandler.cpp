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

#include <Aggregation/SerializableAggregationOperatorHandler.hpp>
#include <Aggregation/SerializableAggregationSlice.hpp>
#include <Nautilus/State/Conversion/AggregationStateConverters.hpp>

#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>

namespace NES
{

SerializableAggregationOperatorHandler::SerializableAggregationOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), true)
{
    // Initialize state with default values
    state_.metadata.operatorId = 0; // Will be set by the system
    state_.metadata.operatorType = 1; // Aggregation operator type
    state_.metadata.processedRecords = 0;
    state_.metadata.lastWatermark = 0;
    state_.metadata.version = 1;

    // Set default configuration
    state_.config.keySize = sizeof(int64_t);
    state_.config.valueSize = sizeof(int64_t);
    state_.config.numberOfBuckets = 1024;
    state_.config.pageSize = 4096;
    
    initializeState();
}

SerializableAggregationOperatorHandler::SerializableAggregationOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const State::AggregationState& initialState)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), true)
    , state_(initialState)
{
    initializeState();
}

TupleBuffer SerializableAggregationOperatorHandler::serialize(AbstractBufferProvider* bufferProvider) const
{
    // Update final metadata before serialization
    const_cast<SerializableAggregationOperatorHandler*>(this)->state_.metadata.processedRecords = 
        state_.metadata.processedRecords;

    // Use the StateSerializer from Phase 1 for automatic serialization
    return Serialization::StateSerializer<State::AggregationState>::serialize(state_, bufferProvider);
}

std::shared_ptr<SerializableAggregationOperatorHandler> 
SerializableAggregationOperatorHandler::deserialize(
    const TupleBuffer& buffer,
    const std::vector<OriginId>& inputOrigins,
    OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
{
    // Deserialize the state using StateSerializer
    auto stateResult = Serialization::StateSerializer<State::AggregationState>::deserialize(buffer);
    if (!stateResult) {
        NES_ERROR("Failed to deserialize AggregationState from buffer");
        throw std::runtime_error("Failed to deserialize AggregationState");
    }

    // Create new handler with the deserialized state and rehydrate runtime slices
    auto handler = std::make_shared<SerializableAggregationOperatorHandler>(
        inputOrigins, outputOriginId, std::move(sliceAndWindowStore), stateResult.value());
    handler->rehydrateFromState();
    return handler;
}

void SerializableAggregationOperatorHandler::captureState()
{
    // If slice store has nothing observable (e.g., tests manually seeded state), keep existing state
    auto windowsToSlices = sliceAndWindowStore->getAllNonTriggeredSlices();
    if (windowsToSlices.empty()) {
        if (watermarkProcessorBuild) {
            state_.metadata.lastWatermark = watermarkProcessorBuild->getCurrentWatermark().getRawValue();
        }
        return;
    }

    // Reset serializable state containers
    state_.hashMaps.clear();
    state_.windows.clear();

    const uint64_t workerCount = numberOfWorkerThreads ? numberOfWorkerThreads : 1;
    uint64_t totalProcessed = 0;

    // Collect all non-triggered slices grouped by window

    for (const auto& [winInfoSeq, slices] : windowsToSlices)
    {
        const auto startIndex = static_cast<uint64_t>(state_.hashMaps.size());
        uint64_t countForWindow = 0;

        // Iterate all slices for this window
        for (const auto& slice : slices)
        {
            auto serSlice = std::dynamic_pointer_cast<SerializableAggregationSlice>(slice);
            if (!serSlice) { continue; }

            // For each worker thread, capture its hash map state
            for (uint64_t tid = 0; tid < workerCount; ++tid)
            {
                const auto& hmState = serSlice->getHashMapState(WorkerThreadId(tid));
                auto asState = NES::State::toState(hmState);
                totalProcessed += asState.tupleCount;
                state_.hashMaps.emplace_back(std::move(asState));
                ++countForWindow;
            }
        }

        // Record window entry
        NES::State::AggregationState::Window w{};
        w.start = winInfoSeq.windowInfo.windowStart.getRawValue();
        w.end = winInfoSeq.windowInfo.windowEnd.getRawValue();
        w.state = 0;
        w.firstHashMapIndex = startIndex;
        w.hashMapCount = countForWindow;
        state_.windows.emplace_back(std::move(w));
    }

    // Update metadata
    state_.metadata.processedRecords = totalProcessed;
    if (watermarkProcessorBuild) {
        state_.metadata.lastWatermark = watermarkProcessorBuild->getCurrentWatermark().getRawValue();
    }
}

void SerializableAggregationOperatorHandler::rehydrateFromState()
{
    // Infer number of worker threads if not set
    if (numberOfWorkerThreads == 0) {
        for (const auto& w : state_.windows) {
            if (w.hashMapCount > 0) { numberOfWorkerThreads = w.hashMapCount; break; }
        }
        if (numberOfWorkerThreads == 0) { numberOfWorkerThreads = 1; }
    }

    // Prepare slice creation args; cleanup not used for offset-based wrapper
    std::vector<std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec>> noop(1);
    CreateNewHashMapSliceArgs args{
        std::move(noop),
        state_.config.keySize,
        state_.config.valueSize,
        state_.config.pageSize,
        state_.config.numberOfBuckets};

    auto makeSlice = getCreateNewSlicesFunction(args);

    for (const auto& w : state_.windows) {
        auto slices = sliceAndWindowStore->getSlicesOrCreate(Timestamp(w.start), makeSlice);
        if (slices.empty()) { continue; }
        auto serSlice = std::dynamic_pointer_cast<SerializableAggregationSlice>(slices[0]);
        if (!serSlice) { continue; }

        const uint64_t base = w.firstHashMapIndex;
        const uint64_t n = std::min<uint64_t>(w.hashMapCount, numberOfWorkerThreads);
        for (uint64_t i = 0; i < n; ++i) {
            auto& target = serSlice->getHashMapState(WorkerThreadId(i));
            target = NES::State::fromState(state_.hashMaps[base + i]);
        }
    }
}

DataStructures::OffsetHashMapWrapper* SerializableAggregationOperatorHandler::getOffsetHashMapWrapper(WorkerThreadId threadId)
{
    auto threadIdx = threadId.getRawValue();
    
    // Ensure we have enough hash map wrappers for all worker threads
    if (threadIdx >= hashMapWrappers_.size()) {
        hashMapWrappers_.resize(threadIdx + 1);
    }
    
    // Ensure we have enough hash map states for all worker threads
    if (threadIdx >= state_.hashMaps.size()) {
        state_.hashMaps.resize(threadIdx + 1);
        
        // Initialize new hash map states
        for (size_t i = hashMapWrappers_.size(); i < state_.hashMaps.size(); ++i) {
            initializeHashMapState(state_.hashMaps[i]);
        }
    }
    
    // Lazy initialization of OffsetHashMapWrapper - share state with handler's state
    if (!hashMapWrappers_[threadIdx]) {
        // Cast the state to OffsetHashMapState since they're structurally identical
        auto& hashMapState = reinterpret_cast<DataStructures::OffsetHashMapState&>(state_.hashMaps[threadIdx]);
        hashMapWrappers_[threadIdx] = std::make_unique<DataStructures::OffsetHashMapWrapper>(hashMapState);
    }
    
    return hashMapWrappers_[threadIdx].get();
}

const DataStructures::OffsetHashMapWrapper* SerializableAggregationOperatorHandler::getOffsetHashMapWrapper(WorkerThreadId threadId) const
{
    return const_cast<SerializableAggregationOperatorHandler*>(this)->getOffsetHashMapWrapper(threadId);
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
SerializableAggregationOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    // Be tolerant in recovery: if not set yet, default to 1
    auto workerThreads = numberOfWorkerThreads == 0 ? 1 : numberOfWorkerThreads;

    // Create a serializable version of the slice creation arguments
    const auto& newHashMapArgs = dynamic_cast<const CreateNewHashMapSliceArgs&>(newSlicesArguments);
    
    return std::function(
        [outputOriginId = outputOriginId, 
         numberOfWorkerThreads = workerThreads, 
         config = state_.config,
         copyOfNewHashMapArgs = newHashMapArgs](
            SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE("Creating new serializable aggregation slice for slice {}-{} for output origin {}", 
                     sliceStart, sliceEnd, outputOriginId);
            
            return {std::make_shared<SerializableAggregationSlice>(
                sliceStart, sliceEnd, copyOfNewHashMapArgs, numberOfWorkerThreads, config)};
        });
}

void SerializableAggregationOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    NES_DEBUG("SerializableAggregationOperatorHandler::triggerSlices called with {} windows", slicesAndWindowInfo.size());
    
    // Now follows the same pattern as AggregationOperatorHandler using HashMap interface
    
    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        NES_DEBUG("Processing window [{}, {}] with {} slices", 
                  windowInfo.windowInfo.windowStart, windowInfo.windowInfo.windowEnd, allSlices.size());
        
        // Collect all hashmaps for each slice that has at least one tuple
        std::unique_ptr<Nautilus::Interface::HashMap> finalHashMap;
        std::vector<Nautilus::Interface::HashMap*> allHashMaps;
        uint64_t totalNumberOfTuples = 0;
        
        for (const auto& slice : allSlices)
        {
            NES_DEBUG("Processing slice: {}", static_cast<void*>(slice.get()));
            const auto serializableAggregationSlice = std::dynamic_pointer_cast<SerializableAggregationSlice>(slice);
            if (!serializableAggregationSlice) {
                NES_WARNING("Failed to cast slice to SerializableAggregationSlice");
                continue;
            }
            
            auto numHashMaps = serializableAggregationSlice->getNumberOfHashMaps();
            NES_DEBUG("Slice has {} hashmaps", numHashMaps);
            
            for (uint64_t hashMapIdx = 0; hashMapIdx < numHashMaps; ++hashMapIdx)
            {
                NES_DEBUG("Checking hashMap index {}", hashMapIdx);
                auto* hashMap = serializableAggregationSlice->getHashMapPtr(WorkerThreadId(hashMapIdx));
                
                if (hashMap != nullptr) {
                    auto tupleCount = hashMap->getNumberOfTuples();
                    NES_DEBUG("HashMap {} has {} tuples", hashMapIdx, tupleCount);
                    
                    // Debug: Add printf to ensure debug is visible
                    printf("TRIGGER_DEBUG: HashMap[%lu] has %lu tuples\n", hashMapIdx, tupleCount);
                    fflush(stdout);
                    
                    if (tupleCount > 0) {
                        NES_DEBUG("Adding non-empty hashMap to allHashMaps");
                        printf("TRIGGER_DEBUG: Adding non-empty hashMap[%lu] to collection\n", hashMapIdx);
                        fflush(stdout);
                        allHashMaps.emplace_back(hashMap);
                        totalNumberOfTuples += tupleCount;
                        if (!finalHashMap)
                        {
                            NES_DEBUG("Creating final OffsetHashMapWrapper");
                            printf("TRIGGER_DEBUG: Creating final OffsetHashMapWrapper\n");
                            fflush(stdout);
                            // Create a new OffsetHashMapWrapper for the final aggregation
                            finalHashMap = std::make_unique<DataStructures::OffsetHashMapWrapper>(
                                state_.config.keySize,
                                state_.config.valueSize, 
                                state_.config.numberOfBuckets
                            );
                            NES_DEBUG("Created final hashMap: {}", static_cast<void*>(finalHashMap.get()));
                            printf("TRIGGER_DEBUG: Created final hashMap: %p\n", static_cast<void*>(finalHashMap.get()));
                            fflush(stdout);
                        }
                    } else {
                        printf("TRIGGER_DEBUG: HashMap[%lu] is empty, skipping\n", hashMapIdx);
                        fflush(stdout);
                    }
                } else {
                    NES_DEBUG("HashMap {} is null", hashMapIdx);
                    printf("TRIGGER_DEBUG: HashMap[%lu] is null\n", hashMapIdx);
                    fflush(stdout);
                }
            }
        }

        printf("TRIGGER_DEBUG: Window processing summary: totalNumberOfTuples=%lu, allHashMaps.size()=%lu\n", 
               totalNumberOfTuples, allHashMaps.size());
        fflush(stdout);
        
        if (totalNumberOfTuples == 0) {
            // No tuples to process, skip this window
            printf("TRIGGER_DEBUG: No tuples to process, skipping this window\n");
            fflush(stdout);
            continue;
        }

        // Calculate buffer size needed for the emission
        const auto neededBufferSize = sizeof(EmittedSerializableAggregationWindow) + 
                                     (allHashMaps.size() * sizeof(Nautilus::Interface::HashMap*));
        
        const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
        if (!tupleBufferVal.has_value())
        {
            throw CannotAllocateBuffer("{}B for the serializable aggregation window trigger were requested", neededBufferSize);
        }

        auto tupleBuffer = tupleBufferVal.value();
        
        // Zero the buffer memory to avoid undefined behavior
        std::memset(tupleBuffer.getBuffer(), 0, neededBufferSize);
        
        // Set buffer metadata - critical for watermark processing
        tupleBuffer.setOriginId(outputOriginId);
        tupleBuffer.setSequenceNumber(windowInfo.sequenceNumber);
        tupleBuffer.setChunkNumber(ChunkNumber(ChunkNumber::INITIAL));
        tupleBuffer.setLastChunk(true);
        tupleBuffer.setWatermark(windowInfo.windowInfo.windowStart);
        tupleBuffer.setNumberOfTuples(totalNumberOfTuples);

        // Create the emitted window structure
        auto* emittedWindow = tupleBuffer.getBuffer<EmittedSerializableAggregationWindow>();
        emittedWindow->windowInfo = windowInfo.windowInfo;
        emittedWindow->numberOfHashMaps = allHashMaps.size();
        emittedWindow->finalHashMap = finalHashMap.release();
        auto* addressFirstHashMapPtr = reinterpret_cast<int8_t*>(emittedWindow) + sizeof(EmittedSerializableAggregationWindow);
        emittedWindow->hashMaps = reinterpret_cast<Nautilus::Interface::HashMap**>(addressFirstHashMapPtr);

        // Copy hashmap interface pointers using memcpy for safety
        std::memcpy(addressFirstHashMapPtr, allHashMaps.data(), allHashMaps.size() * sizeof(Nautilus::Interface::HashMap*));

        // Update processed records count
        state_.metadata.processedRecords += totalNumberOfTuples;

        // Emit the tuple buffer to the probe pipeline
        pipelineCtx->emitBuffer(tupleBuffer, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
}

void SerializableAggregationOperatorHandler::initializeState()
{
    initialized_ = true;
    initializeHashMaps();
}

void SerializableAggregationOperatorHandler::initializeHashMaps()
{
    // Initialize hash maps if not already done
    if (state_.hashMaps.empty() && numberOfWorkerThreads > 0) {
        state_.hashMaps.resize(numberOfWorkerThreads);
        hashMapWrappers_.resize(numberOfWorkerThreads);
        
        for (auto& hashMapState : state_.hashMaps) {
            initializeHashMapState(hashMapState);
        }
    }
}

void SerializableAggregationOperatorHandler::initializeHashMapState(State::AggregationState::HashMap& hashMapState)
{
    hashMapState.keySize = state_.config.keySize;
    hashMapState.valueSize = state_.config.valueSize;
    hashMapState.bucketCount = state_.config.numberOfBuckets;
    hashMapState.entrySize = sizeof(DataStructures::OffsetEntry) + hashMapState.keySize + hashMapState.valueSize;
    hashMapState.chains.resize(hashMapState.bucketCount, 0);
    hashMapState.memory.reserve(state_.config.pageSize);
    hashMapState.tupleCount = 0;
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
SerializableAggregationOperatorHandler::createOffsetBasedSlices(const CreateNewSlicesArguments& args) const
{
    const auto& newHashMapArgs = dynamic_cast<const CreateNewHashMapSliceArgs&>(args);
    
    return [this, copyOfNewHashMapArgs = newHashMapArgs](SliceStart start, SliceEnd end) -> std::vector<std::shared_ptr<Slice>> {
        return {std::make_shared<SerializableAggregationSlice>(start, end, copyOfNewHashMapArgs, numberOfWorkerThreads, state_.config)};
    };
}

}
