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
#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/internal/apex_memmove.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#ifdef __x86_64__
#include <Runtime/internal/rte_memory.h>
#endif
#include <Sources/StaticDataSource.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#ifdef NES_ENABLE_NUMA_SUPPORT
#if defined(__linux__)
#include <numa.h>
#include <numaif.h>
#endif
#endif
#include <fstream>
#include <utility>

namespace NES::Experimental {

StaticDataSource::StaticDataSource(SchemaPtr schema,
                                   std::string pathTableFile,
                                   const bool lateStart,
                                   ::NES::Runtime::BufferManagerPtr bufferManager,
                                   ::NES::Runtime::QueryManagerPtr queryManager,
                                   OperatorId operatorId,
                                   OriginId originId,
                                   size_t numSourceLocalBuffers,
                                   const std::string& physicalSourceName,
                                   std::vector<::NES::Runtime::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      0,// todo  <-- dumb
                      operatorId,
                      originId,
                      numSourceLocalBuffers,
                      GatheringMode::INTERVAL_MODE,// todo: this is a placeholder. gathering mode is unnecessary for static data.
                      std::move(successors),
                      physicalSourceName),
      lateStart(lateStart), pathTableFile(pathTableFile) {

    NES_ASSERT(this->schema, "StaticDataSource: Invalid schema passed.");
    tupleSizeInBytes = this->schema->getSchemaSizeInBytes();
    NES_DEBUG("StaticDataSource: id {} Initialize source with schema: |{}| size: {}",
              std::to_string(operatorId),
              this->schema->toString(),
              std::to_string(tupleSizeInBytes));

    this->sourceAffinity = sourceAffinity;
    bufferSize = localBufferManager->getBufferSize();

    input.open(this->pathTableFile);
    NES_ASSERT(input.is_open(),
               "StaticDataSource: "
               "The following path is not a valid table file: "
                   + pathTableFile);

    // check how many rows are in file/ table
    numTuples = std::count(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>(), '\n');

    // reset ifstream to beginning of file
    input.seekg(0, input.beg);

    numTuplesPerBuffer = bufferSize / tupleSizeInBytes;
    numberOfBuffersToProduce = numTuples / numTuplesPerBuffer + (numTuples % numTuplesPerBuffer != 0);

    // setup file parser
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : this->schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
    std::string delimiter = "|";
    inputParser = std::make_shared<CSVParser>(this->schema->getSize(), physicalTypes, delimiter);

    NES_DEBUG(
        "StaticDataSource() operatorId: {}: eagerLoading={} numTuplesToProcess={} numBuffersToProcess={} numTuplesPerBuffer={}",
        operatorId,
        eagerLoading,
        numTuples,
        numberOfBuffersToProduce,
        numTuplesPerBuffer);

    if (eagerLoading) {
        this->numSourceLocalBuffers = this->numberOfBuffersToProduce;
        preloadBuffers();
    }
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                   .count();

    NES_DEBUG("StaticDataSource created. Timestamp: {}", now);
}

bool StaticDataSource::start() {
    std::unique_lock lock(startConfigMutex);

    startCalled = true;
    if (lateStart) {
        NES_DEBUG("StaticDataSource::start called while lateStart==true. Will start at StartSourceEvent. operatorId: {}",
                  this->operatorId);
        return true;// we didn't start but still signal a success
    }

    lock.unlock();
    NES_DEBUG("StaticDataSource::start called; lateStart==false. Starting now. operatorId: {}", this->operatorId);
    return startStaticDataSourceManually();
}

bool StaticDataSource::startStaticDataSourceManually() { return DataSource::start(); }

void StaticDataSource::onEvent(Runtime::BaseEvent& event) {
    std::unique_lock lock(startConfigMutex);

    NES_DEBUG("StaticDataSource::onEvent(event) called. operatorId: {}", this->operatorId);
    if (event.getEventType() == Runtime::EventType::kStartSourceEvent) {
        NES_DEBUG("StaticDataSource: received startSourceEvent. operatorId: {}", this->operatorId);
        if (startCalled) {
            lock.unlock();
            NES_DEBUG("StaticDataSource::onEvent: start() method was previously called but delayed. Starting source now.");
            startStaticDataSourceManually();
        } else {
            NES_DEBUG(
                "StaticDataSource::onEvent: Received start event. As soon as DataSource::start is called the source will start.");
            lateStart = false;
        }
    } else {
        NES_DEBUG("StaticDataSource received an unknown event.");
    }
}

void StaticDataSource::open() {
    // in the case of eager loading the static data source has already been opened
    if (!eagerLoading) {
        DataSource::open();
    }

    // but we might want to wait for the preloading to finish, for benchmarking reasons:
    if (eagerLoading && this->bufferManager != nullptr// <- has previously been opened
        && onlySendDataWhenLoadingIsFinished) {
        while (filledBuffers.size() < numberOfBuffersToProduce) {
            // this will stall the start of runningRouting()
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

void StaticDataSource::preloadBuffers() {
    // open source, register bufferManager:
    DataSource::open();

    // preload buffers:
    for (size_t i = 0; i < numberOfBuffersToProduce; ++i) {
        auto dynamicBuffer = DataSource::allocateBuffer();
        fillBuffer(dynamicBuffer);
        filledBuffers.push_back(dynamicBuffer.getBuffer());
    }
}

std::optional<::NES::Runtime::TupleBuffer> StaticDataSource::receiveData() {
    NES_DEBUG("StaticDataSource::receiveData called on {}", operatorId);
    if (numberOfBuffersToProduce == numBuffersEmitted || numTuples == numTuplesEmitted) {
        NES_DEBUG("StaticDataSource::receiveData: All data emitted, return nullopt");
        return std::nullopt;
    }

    if (eagerLoading) {
        // todo alternatively we could keep buffers and not recycle them
        NES_DEBUG("StaticDataSource::receiveData: Emit preloaded buffer.");
        NES_ASSERT2_FMT(!filledBuffers.empty(), "StaticDataSource buffers should be preloaded.");
        auto buffer = filledBuffers.front();
        filledBuffers.erase(filledBuffers.begin());
        return buffer;
    }

    NES_DEBUG("StaticDataSource::receiveData: Read and emit new buffer from {}", pathTableFile);
    auto dynamicBuffer = DataSource::allocateBuffer();
    fillBuffer(dynamicBuffer);
    return dynamicBuffer.getBuffer();
}

void StaticDataSource::fillBuffer(::NES::Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    NES_DEBUG("StaticDataSource::fillBuffer: start at pos={}", currentPositionInFile);
    if (this->fileEnded) {
        NES_WARNING("StaticDataSource::fillBuffer: but file has already ended");
        buffer.setNumberOfTuples(0);
        return;
    }
    input.seekg(currentPositionInFile, std::ifstream::beg);

    uint64_t generatedTuplesThisPass = 0;
    //fill buffer maximally
    NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSizeInBytes < buffer.getBuffer().getBufferSize(), "Wrong parameters");
    NES_DEBUG("StaticDataSource::fillBuffer: fill buffer with #tuples= {}  of size= {}", numTuplesPerBuffer, tupleSizeInBytes);

    std::string line;
    uint64_t tupleCount = 0;

    while (tupleCount < numTuplesPerBuffer) {
        if (generatedTuples >= numTuples) {
            fileEnded = true;
            break;
        }
        std::getline(input, line);
        NES_TRACE("StaticDataSource line={}, val={}", tupleCount, line);
        inputParser->writeInputTupleToTupleBuffer(line, tupleCount, buffer, schema, localBufferManager);
        ++tupleCount;
        ++generatedTuples;
    }

    currentPositionInFile = input.tellg();
    buffer.setNumberOfTuples(tupleCount);
    generatedBuffers++;
    NES_DEBUG("StaticDataSource::fillBuffer: reading finished read {} tuples at posInFile={}", tupleCount, currentPositionInFile);
    NES_TRACE("StaticDataSource::fillBuffer: read filled buffer={} ", Util::printTupleBufferAsCSV(buffer.getBuffer(), schema));
}

std::string StaticDataSource::toString() const {
    std::stringstream ss;
    ss << "STATIC_DATA_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << pathTableFile << " numTuples=" << this->numTuples
       << " numBuff=" << this->numberOfBuffersToProduce << ")";
    return ss.str();
}

NES::SourceType StaticDataSource::getType() const { return SourceType::STATIC_DATA_SOURCE; }
}// namespace NES::Experimental