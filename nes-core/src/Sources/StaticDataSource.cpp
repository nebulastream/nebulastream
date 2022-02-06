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
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/internal/apex_memmove.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#ifdef __x86_64__
#include <Runtime/internal/rte_memory.h>
#endif
#include <Sources/StaticDataSource.hpp>
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
#include <fstream>

namespace NES::Experimental {

StaticDataSource::StaticDataSource(SchemaPtr schema,
                           std::string pathTableFile,
                           ::NES::Runtime::BufferManagerPtr bufferManager,
                           ::NES::Runtime::QueryManagerPtr queryManager,
                           OperatorId operatorId,
                           size_t numSourceLocalBuffers,
                           std::vector<::NES::Runtime::Execution::SuccessorExecutablePipeline> successors)
    : GeneratorSource(std::move(schema),
                      std::move(bufferManager),
                      std::move(queryManager),
                      0, // todo  <-- dumb
                      operatorId,
                      numSourceLocalBuffers,
                      GatheringMode::FREQUENCY_MODE, // todo: this is a placeholder. gathering mode is unnecessary for static data.
                      std::move(successors)),
                      pathTableFile(pathTableFile),
                      currentPositionInBytes(0) {

    NES_ASSERT(this->schema, "StaticDataSource: Invalid schema passed.");
    tupleSizeInBytes = this->schema->getSchemaSizeInBytes();
    NES_DEBUG("StaticDataSource: Initialize table with schema: |" + this->schema->toString() + "| size: " + std::to_string(tupleSizeInBytes));

    this->sourceAffinity = sourceAffinity;
    tupleSizeInBytes = this->schema->getSchemaSizeInBytes();
    bufferSize = localBufferManager->getBufferSize();

    std::ifstream input;
    input.open(this->pathTableFile);
    NES_ASSERT(input.is_open(), "StaticDataSource: "
                                "The following path is not a valid table file: " + pathTableFile);

    // check how many rows are in file/ table
    numTuples = std::count(std::istreambuf_iterator<char>(input),
                               std::istreambuf_iterator<char>(), '\n');

    // reset ifstream to beginning of file
    input.seekg(0, input.beg);

    memoryAreaSize = tupleSizeInBytes * numTuples;
    uint8_t* memoryAreaPtr = reinterpret_cast<uint8_t *>(malloc(memoryAreaSize));
    memoryArea = static_cast<const std::shared_ptr<uint8_t>>(memoryAreaPtr);


    // setup a dynamic buffer around the memoryArea, so that the CSV parser can fill it
    auto rowLayout = ::NES::Runtime::MemoryLayouts::RowLayout::create(this->schema, memoryAreaSize);
    auto wrappedMemory = ::NES::Runtime::TupleBuffer::wrapMemory(memoryAreaPtr, memoryAreaSize, this);
    auto dynamicBuffer = ::NES::Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, wrappedMemory);

    // setup file parser
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : this->schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
    std::string delimiter = "|";
    auto inputParser = std::make_shared<CSVParser>(this->schema->getSize(), physicalTypes, delimiter);

    // read file into memory
    std::string line;
    for (size_t tupleCount = 0; tupleCount < numTuples; tupleCount++) {
        std::getline(input, line);
        NES_TRACE("StaticDataSource line=" << tupleCount << " val=" << line);
        inputParser->writeInputTupleToTupleBuffer(line, tupleCount, dynamicBuffer, this->schema);
    }

    // if the memory area is smaller than a buffer
    if (memoryAreaSize <= bufferSize) {
        numTuplesPerBuffer = std::floor(double(memoryAreaSize) / double(this->tupleSizeInBytes));
    } else {
        //if the memory area spans multiple buffers
        auto restTuples = (memoryAreaSize - currentPositionInBytes) / this->tupleSizeInBytes;
        auto numberOfTuplesPerBuffer = std::floor(double(bufferSize) / double(this->tupleSizeInBytes));
        if (restTuples > numberOfTuplesPerBuffer) {
            numTuplesPerBuffer = numberOfTuplesPerBuffer;
        } else {
            numTuplesPerBuffer = restTuples;
        }
    }

    // we know how many buffers this static source contains.
    // the last buffer might not be full but every tuple will get emitted.
    numTuplesPerBuffer = bufferSize / tupleSizeInBytes;
    this->numBuffersToProcess = (numTuples + numTuplesPerBuffer - 1) / numTuplesPerBuffer; // same div trick as above


    NES_DEBUG("StaticDataSource() memoryAreaSize=" << memoryAreaSize);
    NES_ASSERT(memoryArea && memoryAreaSize > 0, "invalid memory area");
}

std::optional<::NES::Runtime::TupleBuffer> StaticDataSource::receiveData() {
    NES_DEBUG("StaticDataSource::receiveData called on " << operatorId);
    auto buffer = this->bufferManager->getBufferBlocking();
    fillBuffer(buffer);
    NES_DEBUG("StaticDataSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer;
}

void StaticDataSource::fillBuffer(::NES::Runtime::TupleBuffer& buffer) {
    NES_DEBUG("StaticDataSource::fillBuffer: start at pos=" << currentPositionInBytes << " memoryAreaSize=" << memoryAreaSize);
    if (generatedTuples >= numTuples) {
        NES_WARNING("StaticDataSource::fillBuffer: but file has already ended");
        buffer.setNumberOfTuples(0);
        return;
    }

    uint64_t generatedTuplesThisPass = 0;
    // fill buffer maximally
    if (numTuplesPerBuffer == 0) {
        generatedTuplesThisPass = buffer.getBufferSize() / tupleSizeInBytes;
    } else {
        generatedTuplesThisPass = numTuplesPerBuffer;
    }
    // with all tuples remaining
    if (generatedTuples + generatedTuplesThisPass > numTuples) {
        generatedTuplesThisPass = numTuples - generatedTuples;
    }
    NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSizeInBytes < buffer.getBufferSize(), "Wrong parameters");

    NES_DEBUG("StaticDataSource::fillBuffer: fill buffer with #tuples=" << generatedTuplesThisPass << " of size=" << tupleSizeInBytes);

    uint8_t *tmp = memoryArea.get() + currentPositionInBytes;
    memcpy(buffer.getBuffer(), tmp, tupleSizeInBytes * generatedTuplesThisPass);

    buffer.setNumberOfTuples(generatedTuplesThisPass);
    generatedTuples += generatedTuplesThisPass;
    currentPositionInBytes = generatedTuples * tupleSizeInBytes;
    generatedBuffers++;

    NES_TRACE("StaticDataSource::fillBuffer: emitted buffer. generatedBuffers=" << generatedBuffers << " generatedTuples="
                << generatedTuples << " currentPositionInBytes=" << currentPositionInBytes);
    NES_TRACE("StaticDataSource::fillBuffer: read produced buffer= " << Util::printTupleBufferAsCSV(buffer, schema));
}

std::string StaticDataSource::toString() const {
    std::stringstream ss;
    ss << "STATIC_DATA_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << pathTableFile
       << " numTuples=" << this->numTuples << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

NES::SourceType StaticDataSource::getType() const { return STATIC_DATA_SOURCE; }
}// namespace NES::Experimental