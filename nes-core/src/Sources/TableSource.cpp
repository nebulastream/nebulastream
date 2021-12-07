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
#ifdef __x86_64__
#include <Runtime/internal/rte_memory.h>
#endif
#include <Sources/TableSource.hpp>
#include <Util/Logger.hpp>
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

TableSource::TableSource(SchemaPtr schema,
                           Runtime::BufferManagerPtr bufferManager,
                           Runtime::QueryManagerPtr queryManager,
                           OperatorId operatorId,
                           size_t numSourceLocalBuffers,
                           std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      0, // todo  <-- dumb
                      operatorId,
                      numSourceLocalBuffers,
                      GatheringMode::FREQUENCY_MODE, // todo: this is a placeholder. gathering mode is unnecessary for static data.
                      std::move(successors)),
      currentPositionInBytes(0),
      numTuples(300) { // todo  <-- dumb
    this->sourceAffinity = sourceAffinity;
    tupleSizeInBytes = this->schema->getSchemaSizeInBytes();
    bufferSize = localBufferManager->getBufferSize();
    NES_DEBUG("schema: |" + this->schema->toString() + "| size:" + std::to_string(tupleSizeInBytes));

    NES_ASSERT(this->schema->toString() == "table_stream$id:INTEGER table_stream$table_col_1:INTEGER table_stream$table_col_2:INTEGER ",
               "wrong schema!");
    NES_ASSERT(tupleSizeInBytes == 24,
               "wrong schema!");


    // todo. for now we manually allocate and fill our "table"
    memoryAreaSize = tupleSizeInBytes * numTuples;
    auto* tmp = reinterpret_cast<uint8_t *>(malloc(memoryAreaSize));
    memoryArea = static_cast<const std::shared_ptr<uint8_t>>(tmp);

    struct Record {
        uint64_t id;
        uint64_t table_col_1;
        uint64_t table_col_2;
    };
    static_assert(sizeof(Record) == 24);
    // static_assert(tupleSizeInBytes == sizeof(Record));

    auto* records = reinterpret_cast<Record*>(tmp);
    size_t numRecords = memoryAreaSize / tupleSizeInBytes;
    for (auto i = 0U; i < numRecords; ++i) {
        records[i].id = i;
        records[i].table_col_1 = i + 1000;
        records[i].table_col_2 = i + 2000;
    }

    // continuing with real setup...
    // if the memory area is smaller than a buffer
    if (memoryAreaSize <= bufferSize) {
        numberOfTuplesToProducePerBuffer = std::floor(double(memoryAreaSize) / double(this->tupleSizeInBytes));
    } else {
        //if the memory area spans multiple buffers
        auto restTuples = (memoryAreaSize - currentPositionInBytes) / this->tupleSizeInBytes;
        auto numberOfTuplesPerBuffer = std::floor(double(bufferSize) / double(this->tupleSizeInBytes));
        if (restTuples > numberOfTuplesPerBuffer) {
            numberOfTuplesToProducePerBuffer = numberOfTuplesPerBuffer;
        } else {
            numberOfTuplesToProducePerBuffer = restTuples;
        }
    }

    NES_DEBUG("TableSource() memoryAreaSize=" << memoryAreaSize); // todo <-- dumb
    NES_ASSERT(memoryArea && memoryAreaSize > 0, "invalid memory area");
}


//std::optional<Runtime::TupleBuffer> TableSource::receiveDataMemory() {
//    NES_DEBUG("TableSource::receiveData called on operatorId=" << operatorId);
//
//    if (memoryAreaSize > bufferSize) {
//        if (currentPositionInBytes + numberOfTuplesToProducePerBuffer * tupleSizeInBytes > memoryAreaSize) {
//            if (numBuffersToProcess != 0) {
//                NES_DEBUG("TableSource::receiveData: reset buffer to 0");
//                currentPositionInBytes = 0;
//            } else {
//                NES_DEBUG("TableSource::receiveData: return as mem sry is empty");
//                return std::nullopt;
//            }
//        }
//    }
//
//    NES_ASSERT2_FMT(numberOfTuplesToProducePerBuffer * tupleSizeInBytes <= bufferSize, "value to write is larger than the buffer");
//
//    Runtime::TupleBuffer buffer = bufferManager->getBufferBlocking();
//    memcpy(buffer.getBuffer(), memoryArea.get() + currentPositionInBytes, bufferSize);
//
//    if (memoryAreaSize > bufferSize) {
//        NES_DEBUG("TableSource::receiveData: add offset=" << bufferSize << " to currentpos=" << currentPositionInBytes);
//        currentPositionInBytes += bufferSize;
//    }
//
//    buffer.setNumberOfTuples(numberOfTuplesToProducePerBuffer);
//
//    generatedTuples += buffer.getNumberOfTuples();
//    generatedBuffers++;
//
//    NES_DEBUG("TableSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());
//    if (buffer.getNumberOfTuples() == 0) {
//        return std::nullopt;
//    }
//    return buffer;
//}


std::optional<Runtime::TupleBuffer> TableSource::receiveData() {
    NES_DEBUG("TableSource::receiveData called on " << operatorId);
    auto buffer = this->bufferManager->getBufferBlocking();
    fillBuffer(buffer);
    NES_DEBUG("TableSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer;
}

void TableSource::fillBuffer(Runtime::TupleBuffer& buffer) {
    NES_DEBUG("TableSource::fillBuffer: start at pos=" << currentPositionInBytes << " memoryAreaSize=" << memoryAreaSize);
    if (generatedTuples >= numTuples) {
        NES_WARNING("TableSource::fillBuffer: but file has already ended");
        buffer.setNumberOfTuples(0);
        return;
    }

    uint64_t generatedTuplesThisPass = 0;
    // fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0) {
        generatedTuplesThisPass = buffer.getBufferSize() / tupleSizeInBytes;
    } else {
        generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
    }
    // with all tuples remaining
    if (generatedTuples + generatedTuplesThisPass > numTuples) {
        generatedTuplesThisPass = numTuples - generatedTuples;
    }
    NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSizeInBytes < buffer.getBufferSize(), "Wrong parameters");

    NES_DEBUG("TableSource::fillBuffer: fill buffer with #tuples=" << generatedTuplesThisPass << " of size=" << tupleSizeInBytes);

    uint8_t *tmp = memoryArea.get() + currentPositionInBytes;
    memcpy(buffer.getBuffer(), tmp, tupleSizeInBytes * generatedTuplesThisPass);

    buffer.setNumberOfTuples(generatedTuplesThisPass);
    generatedTuples += generatedTuplesThisPass;
    currentPositionInBytes = generatedTuples * tupleSizeInBytes;
    generatedBuffers++;
//    NES_TRACE("CSVSource::fillBuffer: reading finished read " << tupleCount << " tuples at posInFile=" << );
    NES_TRACE("TableSource::fillBuffer: read produced buffer= " << Util::printTupleBufferAsCSV(buffer, schema));
}

//std::string CSVSource::toString() const {
//    std::stringstream ss;
//    ss << "CSV_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count()
//    << "ms"
//    << " numBuff=" << this->numBuffersToProcess << ")";
//    return ss.str();
//}

std::string TableSource::toString() const { return "TableSource"; }

NES::SourceType TableSource::getType() const { return MEMORY_SOURCE; }
}// namespace NES