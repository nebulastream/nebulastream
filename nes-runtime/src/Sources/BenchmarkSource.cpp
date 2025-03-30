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
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/internal/apex_memmove.hpp>
#ifdef HAS_AVX
#include <Runtime/internal/rte_memory.h>
#endif
#include <Sources/BenchmarkSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
#if defined(__linux__)
#include <numa.h>
#include <numaif.h>
#endif
#endif
#include <Util/magicenum/magic_enum.hpp>
#include <utility>

namespace NES {

BenchmarkSource::BenchmarkSource(SchemaPtr schema,
                                 const std::shared_ptr<uint8_t>& memoryArea,
                                 size_t memoryAreaSize,
                                 Runtime::BufferManagerPtr bufferManager,
                                 Runtime::QueryManagerPtr queryManager,
                                 uint64_t numberOfBuffersToProcess,
                                 uint64_t gatheringValue,
                                 OperatorId operatorId,
                                 OriginId originId,
                                 StatisticId statisticId,
                                 size_t numSourceLocalBuffers,
                                 GatheringMode gatheringMode,
                                 SourceMode sourceMode,
                                 uint64_t sourceAffinity,
                                 uint64_t taskQueueId,
                                 const std::string& physicalSourceName,
                                 std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      numberOfBuffersToProcess,
                      operatorId,
                      originId,
                      statisticId,
                      numSourceLocalBuffers,
                      gatheringMode,
                      std::move(successors),
                      physicalSourceName),
      memoryArea(memoryArea), memoryAreaSize(memoryAreaSize), currentPositionInBytes(0), sourceMode(sourceMode) {
    NES_ASSERT(this->memoryArea && this->memoryAreaSize > 0, "invalid memory area");
    this->numberOfBuffersToProduce = numberOfBuffersToProcess;
    if (gatheringMode == GatheringMode::INTERVAL_MODE) {
        this->gatheringInterval = std::chrono::milliseconds(gatheringValue);
    } else if (gatheringMode == GatheringMode::INGESTION_RATE_MODE) {
        this->gatheringIngestionRate = gatheringValue;
    } else {
        NES_THROW_RUNTIME_ERROR("Mode not implemented " << magic_enum::enum_name(gatheringMode));
    }
    this->sourceAffinity = sourceAffinity;
    this->taskQueueId = taskQueueId;

    schemaSize = this->schema->getSchemaSizeInBytes();
    bufferSize = localBufferManager->getBufferSize();

    //if the memory area is smaller than a buffer
    if (memoryAreaSize <= bufferSize) {
        numberOfTuplesToProduce = std::floor(double(memoryAreaSize) / double(this->schemaSize));
    } else {
        //if the memory area spans multiple buffers
        auto restTuples = memoryAreaSize / this->schemaSize;
        auto numberOfTuplesPerBuffer = std::floor(double(bufferSize) / double(this->schemaSize));
        if (restTuples > numberOfTuplesPerBuffer) {
            numberOfTuplesToProduce = numberOfTuplesPerBuffer;
        } else {
            numberOfTuplesToProduce = restTuples;
        }
    }

    NES_DEBUG("BenchmarkSource() numberOfBuffersToProcess={} memoryAreaSize={}", numberOfBuffersToProcess, memoryAreaSize);
}

BenchmarkSource::~BenchmarkSource() { numaLocalMemoryArea.release(); }

void BenchmarkSource::open() {
    DataSource::open();
    numaLocalMemoryArea = *localBufferManager->getUnpooledBuffer(memoryAreaSize);
    std::memcpy(numaLocalMemoryArea.getBuffer(), memoryArea.get(), memoryAreaSize);
    memoryArea.reset();
    memoryAreaRefCnt = 1;
}

void BenchmarkSource::recyclePooledBuffer(Runtime::detail::MemorySegment*) {
    if (memoryAreaRefCnt.fetch_sub(1) == 1) {
        numaLocalMemoryArea.release();
    }
}

void BenchmarkSource::recycleUnpooledBuffer(Runtime::detail::MemorySegment*) {
    if (memoryAreaRefCnt.fetch_sub(1) == 1) {
        numaLocalMemoryArea.release();
    }
}

void BenchmarkSource::runningRoutine() {
    try {
        open();

        NES_INFO("Going to produce {}", numberOfTuplesToProduce);

        for (uint64_t i = 0; i < numberOfBuffersToProduce && running; ++i) {
            Runtime::TupleBuffer buffer;
            switch (sourceMode) {
                case SourceMode::EMPTY_BUFFER: {
                    buffer = bufferManager->getBufferBlocking();
                    break;
                }
                case SourceMode::COPY_BUFFER_SIMD_RTE: {
#ifdef HAS_AVX
                    buffer = bufferManager->getBufferBlocking();
                    rte_memcpy(buffer.getBuffer(),
                               numaLocalMemoryArea.getBuffer() + currentPositionInBytes,
                               buffer.getBufferSize());
#else
                    NES_THROW_RUNTIME_ERROR("COPY_BUFFER_SIMD_RTE source mode is not supported.");
#endif
                    break;
                }
                case SourceMode::COPY_BUFFER_SIMD_APEX: {
                    buffer = bufferManager->getBufferBlocking();
                    apex_memcpy(buffer.getBuffer(),
                                numaLocalMemoryArea.getBuffer() + currentPositionInBytes,
                                buffer.getBufferSize());
                    break;
                }
                case SourceMode::CACHE_COPY: {
                    buffer = bufferManager->getBufferBlocking();
                    memcpy(buffer.getBuffer(), numaLocalMemoryArea.getBuffer(), buffer.getBufferSize());
                    break;
                }
                case SourceMode::COPY_BUFFER: {
                    buffer = bufferManager->getBufferBlocking();
                    memcpy(buffer.getBuffer(), numaLocalMemoryArea.getBuffer() + currentPositionInBytes, buffer.getBufferSize());
                    break;
                }
                case SourceMode::WRAP_BUFFER: {
//                    buffer = Runtime::TupleBuffer::wrapMemory(numaLocalMemoryArea.getBuffer() + currentPositionInBytes,
//                                                              bufferSize,
//                                                              this);
                    memoryAreaRefCnt.fetch_add(1);
                    break;
                }
            }

            buffer.setNumberOfTuples(numberOfTuplesToProduce);
            generatedTuples += numberOfTuplesToProduce;
            generatedBuffers++;
            currentPositionInBytes += bufferSize;

            for (const auto& successor : *executableSuccessors.rlock()) {
                queryManager->addWorkForNextPipeline(buffer, successor, taskQueueId);
            }
        }

        close();
        completedPromise.set_value(true);
        NES_DEBUG("DataSource {} end running", operatorId);
    } catch (std::exception const& error) {
        queryManager->notifySourceFailure(shared_from_base<DataSource>(), error.what());
        completedPromise.set_exception(std::make_exception_ptr(error));
    }
}

void BenchmarkSource::close() {
    DataSource::close();
    if (memoryAreaRefCnt.fetch_sub(1) == 1) {
        numaLocalMemoryArea.release();
    }
}

std::optional<Runtime::TupleBuffer> BenchmarkSource::receiveData() {
    //the benchmark source does not follow the receive pattern and overwrites the running routine
    NES_NOT_IMPLEMENTED();
    Runtime::TupleBuffer buffer;
    return buffer;
}

std::string BenchmarkSource::toString() const { return "BenchmarkSource"; }

NES::SourceType BenchmarkSource::getType() const { return SourceType::MEMORY_SOURCE; }
}// namespace NES
