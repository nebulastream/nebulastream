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
#ifdef HAS_AVX
#include <Runtime/internal/rte_memory.h>
#endif
#include <Sources/BenchmarkSource.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cmath>
#ifdef NES_ENABLE_NUMA_SUPPORT
#if defined(__linux__)
#include <numa.h>
#include <numaif.h>
#endif
#endif
#include <utility>

namespace NES {

BenchmarkSource::BenchmarkSource(SchemaPtr schema,
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
    schemaSize = this->schema->getSchemaSizeInBytes();
    bufferSize = localBufferManager->getBufferSize();

    //if the memory area is smaller than a buffer
    if (memoryAreaSize <= bufferSize) {
        numberOfTuplesToProduce = std::floor(double(memoryAreaSize) / double(this->schemaSize));
    } else {
        //if the memory area spans multiple buffers
        auto restTuples = (memoryAreaSize - currentPositionInBytes) / this->schemaSize;
        auto numberOfTuplesPerBuffer = std::floor(double(bufferSize) / double(this->schemaSize));
        if (restTuples > numberOfTuplesPerBuffer) {
            numberOfTuplesToProduce = numberOfTuplesPerBuffer;
        } else {
            numberOfTuplesToProduce = restTuples;
        }
    }

    NES_DEBUG("BenchmarkSource() numBuffersToProcess=" << numBuffersToProcess << " memoryAreaSize=" << memoryAreaSize);
    NES_ASSERT(memoryArea && memoryAreaSize > 0, "invalid memory area");
}

void BenchmarkSource::open() {
    DataSource::open();
    auto buffer = localBufferManager->getUnpooledBuffer(memoryAreaSize);
    numaLocalMemoryArea = *buffer;
    std::memcpy(numaLocalMemoryArea.getBuffer(), memoryArea.get(), memoryAreaSize);
    memoryArea.reset();
}

void BenchmarkSource::runningRoutine() {
    open();

    NES_INFO("Going to produce " << numberOfTuplesToProduce);
    std::cout << "Going to produce " << numberOfTuplesToProduce << std::endl;

    for (uint64_t i = 0; i < numBuffersToProcess && running; ++i) {
        Runtime::TupleBuffer buffer;
        switch (sourceMode) {
            case EMPTY_BUFFER: {
                buffer = bufferManager->getBufferBlocking();
                break;
            }
            case COPY_BUFFER_SIMD_RTE: {
#ifdef HAS_AVX
                buffer = bufferManager->getBufferBlocking();
                rte_memcpy(buffer.getBuffer(), numaLocalMemoryArea.getBuffer() + currentPositionInBytes, buffer.getBufferSize());
#else
                NES_THROW_RUNTIME_ERROR("COPY_BUFFER_SIMD_RTE source mode is not supported.");
#endif
                break;
            }
            case COPY_BUFFER_SIMD_APEX: {
                buffer = bufferManager->getBufferBlocking();
                apex_memcpy(buffer.getBuffer(), numaLocalMemoryArea.getBuffer() + currentPositionInBytes, buffer.getBufferSize());
                break;
            }
            case CACHE_COPY: {
                buffer = bufferManager->getBufferBlocking();
                memcpy(buffer.getBuffer(), numaLocalMemoryArea.getBuffer(), buffer.getBufferSize());
                break;
            }
            case COPY_BUFFER: {
                buffer = bufferManager->getBufferBlocking();
                memcpy(buffer.getBuffer(), numaLocalMemoryArea.getBuffer() + currentPositionInBytes, buffer.getBufferSize());
                break;
            }
            case WRAP_BUFFER: {
                buffer =
                    Runtime::TupleBuffer::wrapMemory(numaLocalMemoryArea.getBuffer() + currentPositionInBytes, bufferSize, this);
                break;
            }
        }

        buffer.setNumberOfTuples(numberOfTuplesToProduce);
        generatedTuples += numberOfTuplesToProduce;
        generatedBuffers++;

        for (const auto& successor : executableSuccessors) {
            queryManager->addWorkForNextPipeline(buffer, successor, numaNode);
        }
    }

    close();
    // inject reconfiguration task containing end of stream
    queryManager->addEndOfStream(shared_from_base<DataSource>(), wasGracefullyStopped);//
    bufferManager->destroy();
    queryManager.reset();
    NES_DEBUG("DataSource " << operatorId << " end running");
}

void BenchmarkSource::close() {
    DataSource::close();
    numaLocalMemoryArea.release();
}

std::optional<Runtime::TupleBuffer> BenchmarkSource::receiveData() {
    //the benchmark source does not follow the receive pattern and overwrites the running routine
    NES_NOT_IMPLEMENTED();
    Runtime::TupleBuffer buffer;
    return buffer;
}

std::string BenchmarkSource::toString() const { return "BenchmarkSource"; }

NES::SourceType BenchmarkSource::getType() const { return MEMORY_SOURCE; }
}// namespace NES