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
#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/internal/apex_memmove.hpp>
#include <Sources/Parsers/CSVParser.hpp>
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
#include <fstream>

namespace NES {

TableSource::TableSource(SchemaPtr schema,
                           std::string pathTableFile,
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
                      pathTableFile(pathTableFile),
                      currentPositionInBytes(0) {

    NES_ASSERT(this->schema, "TableSource: Invalid schema passed.");
    tupleSizeInBytes = this->schema->getSchemaSizeInBytes();
    NES_DEBUG("TableSource: Initialize table with schema: |" + this->schema->toString() + "| size: " + std::to_string(tupleSizeInBytes));

    this->sourceAffinity = sourceAffinity;
    tupleSizeInBytes = this->schema->getSchemaSizeInBytes();
    bufferSize = localBufferManager->getBufferSize();

    std::ifstream input;
    input.open(this->pathTableFile);
    NES_ASSERT(input.is_open(), "TableSource: "
                                "The following path is not a valid table file: " + pathTableFile);

    // check how many rows are in file/ table
    numTuples = std::count(std::istreambuf_iterator<char>(input),
                               std::istreambuf_iterator<char>(), '\n');

    // reset ifstream to beginning of file
    input.seekg(0, input.beg);

    memoryAreaSize = tupleSizeInBytes * numTuples;
    uint8_t* tmp = reinterpret_cast<uint8_t *>(malloc(memoryAreaSize));
    memoryArea = static_cast<const std::shared_ptr<uint8_t>>(tmp);

    // setup file parser
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : this->schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
    std::string delimiter = "|";
    auto inputParser = std::make_shared<CSVParser>(tupleSizeInBytes, this->schema->getSize(), physicalTypes, delimiter);

    // read file into memory
    std::string line;
    for (size_t tupleCount = 0; tupleCount < numTuples; tupleCount++) {
        std::getline(input, line);
        NES_TRACE("TableSource line=" << tupleCount << " val=" << line);
        inputParser->writeInputTupleToMemoryArea(line, tupleCount, tmp, memoryAreaSize);
    }

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

    // we know how many buffers this static source contains.
    // the last buffer might not be full but every tuple will get emitted.
    this->numBuffersToProcess = (memoryAreaSize + bufferSize - 1) / bufferSize;

    NES_DEBUG("TableSource() memoryAreaSize=" << memoryAreaSize);
    NES_ASSERT(memoryArea && memoryAreaSize > 0, "invalid memory area");
}

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

NES::SourceType TableSource::getType() const { return TABLE_SOURCE; }
}// namespace NES