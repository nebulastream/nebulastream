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
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace NES {

CSVSource::CSVSource(SchemaPtr schema,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     Configurations::CSVSourceConfigPtr sourceConfigPtr,
                     OperatorId operatorId,
                     size_t numSourceLocalBuffers,
                     GatheringMode gatheringMode,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(successors)),
      sourceConfigPtr(sourceConfigPtr), filePath(sourceConfigPtr->getFilePath()->getValue()),
      numberOfTuplesToProducePerBuffer(sourceConfigPtr->getNumberOfTuplesToProducePerBuffer()->getValue()),
      delimiter(sourceConfigPtr->getDelimiter()->getValue()), skipHeader(sourceConfigPtr->getSkipHeader()->getValue()),
      rowLayout(sourceConfigPtr->getRowLayout()->getValue()) {
    this->numBuffersToProcess = sourceConfigPtr->getNumberOfBuffersToProduce()->getValue();
    this->gatheringInterval = std::chrono::milliseconds(sourceConfigPtr->getSourceFrequency()->getValue());
    this->tupleSize = schema->getSchemaSizeInBytes();

    char* path = realpath(filePath.c_str(), nullptr);
    NES_DEBUG("CSVSource: Opening path " << path);
    input.open(path);

    NES_DEBUG("CSVSource::CSVSource: read buffer");
    input.seekg(0, std::ifstream::end);
    if (auto const reportedFileSize = input.tellg(); reportedFileSize == -1) {
        NES_ERROR("CSVSource::CSVSource File " + filePath + " is corrupted");
        //        NES_ASSERT2_FMT(false, "CSVSource::CSVSource File " + filePath + " is corrupted");
    } else {
        this->fileSize = static_cast<decltype(this->fileSize)>(reportedFileSize);
    }

    this->loopOnFile = sourceConfigPtr->getNumberOfBuffersToProduce()->getValue() == 0;

    NES_DEBUG("CSVSource: tupleSize=" << this->tupleSize << " freq=" << this->gatheringInterval.count() << "ms"
                                      << " numBuff=" << this->numBuffersToProcess << " numberOfTuplesToProducePerBuffer="
                                      << this->numberOfTuplesToProducePerBuffer << "loopOnFile=" << this->loopOnFile);

    this->fileEnded = false;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    this->inputParser = std::make_shared<CSVParser>(schema->getSize(), physicalTypes, delimiter);
}

std::optional<Runtime::TupleBuffer> CSVSource::receiveData() {
    NES_DEBUG("CSVSource::receiveData called on " << operatorId);
    auto tupleBuffer = this->bufferManager->getBufferBlocking();
    if (rowLayout) {
        Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
            Runtime::MemoryLayouts::RowLayout::create(schema, this->bufferManager->getBufferSize());
        Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, tupleBuffer);
        fillBuffer(buffer);
        NES_DEBUG("CSVSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

        if (buffer.getNumberOfTuples() == 0) {
            return std::nullopt;
        }
        return buffer.getBuffer();
    } else {
        Runtime::MemoryLayouts::ColumnLayoutPtr layoutPtr =
            Runtime::MemoryLayouts::ColumnLayout::create(schema, this->bufferManager->getBufferSize());
        Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, tupleBuffer);
        fillBuffer(buffer);
        NES_DEBUG("CSVSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

        if (buffer.getNumberOfTuples() == 0) {
            return std::nullopt;
        }
        return buffer.getBuffer();
    }
}

std::string CSVSource::toString() const {
    std::stringstream ss;
    ss << "CSV_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count()
       << "ms"
       << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

void CSVSource::fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    NES_DEBUG("CSVSource::fillBuffer: start at pos=" << currentPositionInFile << " fileSize=" << fileSize);
    if (this->fileEnded) {
        NES_WARNING("CSVSource::fillBuffer: but file has already ended");
        buffer.setNumberOfTuples(0);
        return;
    }
    input.seekg(currentPositionInFile, std::ifstream::beg);

    uint64_t generatedTuplesThisPass = 0;
    //fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0) {
        generatedTuplesThisPass = buffer.getBuffer().getBufferSize() / tupleSize;
    } else {
        generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
        NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < buffer.getBuffer().getBufferSize(), "Wrong parameters");
    }
    NES_DEBUG("CSVSource::fillBuffer: fill buffer with #tuples=" << generatedTuplesThisPass << " of size=" << tupleSize);

    std::string line;
    uint64_t tupleCount = 0;

    if (skipHeader && currentPositionInFile == 0) {
        NES_DEBUG("CSVSource: Skipping header");
        std::getline(input, line);
        currentPositionInFile = input.tellg();
    }

    while (tupleCount < generatedTuplesThisPass) {
        if (auto const tg = input.tellg(); (tg >= 0 && static_cast<uint64_t>(tg) >= fileSize) || tg == -1) {
            NES_DEBUG("CSVSource::fillBuffer: reset tellg()=" << input.tellg() << " fileSize=" << fileSize);
            input.clear();
            input.seekg(0, std::ifstream::beg);
            if (!this->loopOnFile) {
                NES_DEBUG("CSVSource::fillBuffer: break because file ended");
                this->fileEnded = true;
                break;
            }
            if (this->skipHeader) {
                NES_DEBUG("CSVSource: Skipping header");
                std::getline(input, line);
                currentPositionInFile = input.tellg();
            }
        }

        std::getline(input, line);
        NES_TRACE("CSVSource line=" << tupleCount << " val=" << line);
        // TODO: there will be a problem with non-printable characters (at least with null terminators). Check sources

        inputParser->writeInputTupleToTupleBuffer(line, tupleCount, buffer, schema);
        tupleCount++;
    }//end of while

    currentPositionInFile = input.tellg();
    buffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    NES_TRACE("CSVSource::fillBuffer: reading finished read " << tupleCount << " tuples at posInFile=" << currentPositionInFile);
    NES_TRACE("CSVSource::fillBuffer: read produced buffer= " << Util::printTupleBufferAsCSV(buffer.getBuffer(), schema));
}

SourceType CSVSource::getType() const { return CSV_SOURCE; }

std::string CSVSource::getFilePath() const { return filePath; }

std::string CSVSource::getDelimiter() const { return delimiter; }

uint64_t CSVSource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

bool CSVSource::getSkipHeader() const { return skipHeader; }

const Configurations::CSVSourceConfigPtr& CSVSource::getSourceConfigPtr() const { return sourceConfigPtr; }
}// namespace NES
