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
#include <Sources/DataSource.hpp>
#include <Sources/MaterializedViewSource.hpp>
#ifdef __x86_64__
#include <Runtime/internal/rte_memory.h>
#endif
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

namespace NES::Experimental {

MaterializedViewSource::MaterializedViewSource(SchemaPtr schema,
                                               MaterializedViewPtr mView,
                                               Runtime::BufferManagerPtr bufferManager,
                                               Runtime::QueryManagerPtr queryManager,
                                               uint64_t numBuffersToProcess,
                                               OperatorId operatorId,
                                               size_t numSourceLocalBuffers,
                                               std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      numBuffersToProcess,
                      operatorId,
                      numSourceLocalBuffers,
                      gatheringMode,
                      std::move(successors)),
      mView(mView), currentPositionInBytes(0) {
    const size_t memoryAreaSize = mView.get()->getMemAreaSize();

    this->numBuffersToProcess = numBuffersToProcess;

    // Frequeny not neede here
    /*if (gatheringMode == GatheringMode::FREQUENCY_MODE) {
        this->gatheringInterval = std::chrono::milliseconds(gatheringValue);
    } else if (gatheringMode == GatheringMode::INGESTION_RATE_MODE) {
        this->gatheringIngestionRate = gatheringValue;
    } else {
        NES_THROW_RUNTIME_ERROR("Mode not implemented " << gatheringMode);
    }*/

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

    NES_DEBUG("MaterializedViewSource() numBuffersToProcess=" << numBuffersToProcess << " memoryAreaSize=" << memoryAreaSize);
    NES_ASSERT(mView.get()->getMemArea() && memoryAreaSize > 0, "invalid memory area");
}

std::optional<Runtime::TupleBuffer> MaterializedViewSource::receiveData() {
    NES_DEBUG("MaterializedViewSource::receiveData called on operatorId=" << operatorId);

    if (mView.get()->getMemAreaSize() > bufferSize) {
        if (currentPositionInBytes + numberOfTuplesToProduce * schemaSize > mView.get()->getMemAreaSize()) {
            if (numBuffersToProcess != 0) {
                NES_DEBUG("MaterializedViewSource::receiveData: reset buffer to 0");
                currentPositionInBytes = 0;
            } else {
                NES_DEBUG("MaterializedViewSource::receiveData: return as mem sry is empty");
                return std::nullopt;
            }
        }
    }

    NES_ASSERT2_FMT(numberOfTuplesToProduce * schemaSize <= bufferSize, "value to write is larger than the buffer");

    Runtime::TupleBuffer buffer = bufferManager->getBufferBlocking();
    memcpy(buffer.getBuffer(), mView.get()->getMemArea() + currentPositionInBytes, bufferSize);

    if (mView.get()->getMemAreaSize() > bufferSize) {
        NES_DEBUG("MaterializedViewSource::receiveData: add offset=" << bufferSize
                                                                     << " to currentpos=" << currentPositionInBytes);
        currentPositionInBytes += bufferSize;
    }

    buffer.setNumberOfTuples(numberOfTuplesToProduce);

    generatedTuples += buffer.getNumberOfTuples();
    generatedBuffers++;

    NES_DEBUG("MaterializedViewSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());
    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer;
}

std::string MaterializedViewSource::toString() const { return "MaterializedViewSource"; }

NES::SourceType MaterializedViewSource::getType() const { return MATERIALIZED_VIEW_SOURCE; }
}// namespace NES::Experimental