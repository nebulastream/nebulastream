/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/internal/apex_memmove.hpp>
#include <Runtime/internal/rte_memory.h>
#include <Sources/MemorySource.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cmath>
#include <numa.h>
#include <numaif.h>
#include <utility>

namespace NES {

MemorySource::MemorySource(SchemaPtr schema,
                           const std::shared_ptr<uint8_t>& memoryArea,
                           size_t memoryAreaSize,
                           Runtime::BufferManagerPtr bufferManager,
                           Runtime::QueryManagerPtr queryManager,
                           uint64_t numBuffersToProcess,
                           uint64_t gatheringValue,
                           OperatorId operatorId,
                           size_t numSourceLocalBuffers,
                           GatheringMode gatheringMode,
                           SourceMode sourceMode,
                           uint64_t sourceAffinity,
                           std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      numBuffersToProcess,
                      operatorId,
                      numSourceLocalBuffers,
                      gatheringMode,
                      std::move(successors)),
      memoryArea(memoryArea), memoryAreaSize(memoryAreaSize), currentPositionInBytes(0), sourceMode(sourceMode) {
    this->numBuffersToProcess = numBuffersToProcess;
    if (gatheringMode == GatheringMode::FREQUENCY_MODE) {
        this->gatheringInterval = std::chrono::milliseconds(gatheringValue);
    } else if (gatheringMode == GatheringMode::INGESTION_RATE_MODE) {
        this->gatheringIngestionRate = gatheringValue;
    } else {
        NES_THROW_RUNTIME_ERROR("Mode not implemented " << gatheringMode);
    }
    this->sourceAffinity = sourceAffinity;

    //if the memory area is smaller than a buffer
    if (memoryAreaSize <= globalBufferManager->getBufferSize()) {
        numberOfTuplesToProduce = std::floor(double(memoryAreaSize) / double(this->schema->getSchemaSizeInBytes()));
    } else {
        //if the memory area spans multiple buffers
        auto restTuples = (memoryAreaSize - currentPositionInBytes) / this->schema->getSchemaSizeInBytes();
        auto numberOfTuplesPerBuffer =
            std::floor(double(globalBufferManager->getBufferSize()) / double(this->schema->getSchemaSizeInBytes()));
        if (restTuples > numberOfTuplesPerBuffer) {
            numberOfTuplesToProduce = numberOfTuplesPerBuffer;
        } else {
            numberOfTuplesToProduce = restTuples;
        }
    }

    NES_DEBUG("MemorySource() numBuffersToProcess=" << numBuffersToProcess << " memoryAreaSize=" << memoryAreaSize);
    NES_ASSERT(memoryArea && memoryAreaSize > 0, "invalid memory area");
}

std::optional<Runtime::TupleBuffer> MemorySource::receiveData() {
    NES_DEBUG("MemorySource::receiveData called on operatorId=" << operatorId);
    if (!initialized) {
        numaLocalMemoryArea = numa_alloc_local(memoryAreaSize);
        void* numaLocalMemoryArea2 = numa_alloc_onnode(memoryAreaSize, 0);
        int newNumaNode = -1;
        get_mempolicy(&newNumaNode, NULL, 0, (void*) numaLocalMemoryArea, MPOL_F_NODE | MPOL_F_ADDR);

        int oldNumaNode = -1;
        get_mempolicy(&oldNumaNode, NULL, 0, (void*) memoryArea.get(), MPOL_F_NODE | MPOL_F_ADDR);

        int newFix = -1;
        get_mempolicy(&newFix, NULL, 0, (void*) numaLocalMemoryArea2, MPOL_F_NODE | MPOL_F_ADDR);

        int status[1];
        int ret_code;
        status[0] = -1;
        ret_code = move_pages(0 /*self memory */, 1, &numaLocalMemoryArea, NULL, status, 0);
        printf("Memory at %p is at %d node (retcode %d)\n", numaLocalMemoryArea, status[0], ret_code);

        std::cout << "Mem src move from old numa node=" << oldNumaNode << " to new numa node=" << newNumaNode
                  << " on core=" << sched_getcpu() << " newFix=" << newFix << std::endl;
        initialized = true;
    }

    auto bufferSize = globalBufferManager->getBufferSize();
    if (memoryAreaSize > bufferSize) {
        if (currentPositionInBytes + numberOfTuplesToProduce * schema->getSchemaSizeInBytes() > memoryAreaSize) {
            if (numBuffersToProcess != 0) {
                NES_DEBUG("MemorySource::receiveData: reset buffer to 0");
                currentPositionInBytes = 0;
            } else {
                NES_DEBUG("MemorySource::receiveData: return as mem sry is empty");
                return std::nullopt;
            }
        }
    }

    NES_ASSERT2_FMT(numberOfTuplesToProduce * schema->getSchemaSizeInBytes() <= bufferSize,
                    "value to write is larger than the buffer");

    Runtime::TupleBuffer buffer;
    switch (sourceMode) {
        case EMPTY_BUFFER: {
            buffer = bufferManager->getBufferBlocking();
            break;
        }
        case COPY_BUFFER_SIMD_RTE: {
            buffer = bufferManager->getBufferBlocking();
            rte_memcpy(buffer.getBuffer(), memoryArea.get() + currentPositionInBytes, buffer.getBufferSize());
            break;
        }
        case COPY_BUFFER_SIMD_APEX: {
            buffer = bufferManager->getBufferBlocking();
            apex_memcpy(buffer.getBuffer(), memoryArea.get() + currentPositionInBytes, buffer.getBufferSize());
            break;
        }
        case CACHE_COPY: {
            buffer = bufferManager->getBufferBlocking();
            memcpy(buffer.getBuffer(), numaLocalMemoryArea, buffer.getBufferSize());
            break;
        }
        case COPY_BUFFER: {
            buffer = bufferManager->getBufferBlocking();
            memcpy(buffer.getBuffer(), memoryArea.get() + currentPositionInBytes, buffer.getBufferSize());
            break;
        }
        case WRAP_BUFFER: {
            buffer = Runtime::TupleBuffer::wrapMemory(memoryArea.get() + currentPositionInBytes, bufferSize, this);
            break;
        }
    }

    if (memoryAreaSize > bufferSize) {
        NES_DEBUG("MemorySource::receiveData: add offset=" << bufferSize << " to currentpos=" << currentPositionInBytes);
        currentPositionInBytes += bufferSize;
    }

    buffer.setNumberOfTuples(numberOfTuplesToProduce);

    generatedTuples += buffer.getNumberOfTuples();
    generatedBuffers++;

    NES_DEBUG("MemorySource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());
    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer;
}

std::string MemorySource::toString() const { return "MemorySource"; }

NES::SourceType MemorySource::getType() const { return MEMORY_SOURCE; }
}// namespace NES