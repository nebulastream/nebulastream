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
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
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
                     CSVSourceTypePtr csvSourceType,
                     OperatorId operatorId,
                     OriginId originId,
                     StatisticId statisticId,
                     size_t numSourceLocalBuffers,
                     GatheringMode gatheringMode,
                     const std::string& physicalSourceName,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 statisticId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 physicalSourceName,
                 false,
                 std::move(successors)),
      fileEnded(false), csvSourceType(csvSourceType), filePath(csvSourceType->getFilePath()->getValue()),
      numberOfTuplesToProducePerBuffer(csvSourceType->getNumberOfTuplesToProducePerBuffer()->getValue()),
      delimiter(csvSourceType->getDelimiter()->getValue()), skipHeader(csvSourceType->getSkipHeader()->getValue()) {

    this->numberOfBuffersToProduce = csvSourceType->getNumberOfBuffersToProduce()->getValue();
    this->gatheringInterval = std::chrono::milliseconds(csvSourceType->getGatheringInterval()->getValue());
    this->tupleSize = schema->getSchemaSizeInBytes();

    struct Deleter {
        void operator()(const char* ptr) { std::free(const_cast<char*>(ptr)); }
    };
    auto path = std::unique_ptr<const char, Deleter>(const_cast<const char*>(realpath(filePath.c_str(), nullptr)));
    if (path == nullptr) {
        NES_THROW_RUNTIME_ERROR("Could not determine absolute pathname: " << filePath.c_str());
    }

    input.open(path.get());
    if (!(input.is_open() && input.good())) {
        throw Exceptions::RuntimeException("Cannot open file: " + std::string(path.get()));
    }

    NES_DEBUG("CSVSource: Opening path {}", path.get());
    input.seekg(0, std::ifstream::end);
    if (auto const reportedFileSize = input.tellg(); reportedFileSize == -1) {
        throw Exceptions::RuntimeException("CSVSource::CSVSource File " + filePath + " is corrupted");
    } else {
        this->fileSize = static_cast<decltype(this->fileSize)>(reportedFileSize);
    }

    NES_DEBUG("CSVSource: tupleSize={} freq={}ms numBuff={} numberOfTuplesToProducePerBuffer={}",
              this->tupleSize,
              this->gatheringInterval.count(),
              this->numberOfBuffersToProduce,
              this->numberOfTuplesToProducePerBuffer);

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    this->inputParser = std::make_shared<CSVParser>(schema->getSize(), physicalTypes, delimiter);
}

std::optional<Runtime::TupleBuffer> CSVSource::receiveData() {
    NES_TRACE("CSVSource::receiveData called on  {}", operatorId);
    auto buffer = allocateBuffer();
    fillBuffer(buffer);
    NES_TRACE("CSVSource::receiveData filled buffer with tuples= {}", buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer.getBuffer();
}

std::string CSVSource::toString() const {
    std::stringstream ss;
    ss << "CSV_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << filePath << " freq=" << this->gatheringInterval.count()
       << "ms"
       << " numBuff=" << this->numberOfBuffersToProduce << ")";
    return ss.str();
}

void CSVSource::fillBuffer(Runtime::MemoryLayouts::TestTupleBuffer& buffer) {
    NES_TRACE("CSVSource::fillBuffer: start at pos={} fileSize={}", currentPositionInFile, fileSize);
    if (this->fileEnded) {
        NES_WARNING("CSVSource::fillBuffer: but file has already ended");
        return;
    }

    input.seekg(currentPositionInFile, std::ifstream::beg);

    uint64_t generatedTuplesThisPass = 0;
    //fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0) {
        generatedTuplesThisPass = buffer.getCapacity();
    } else {
        generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
        NES_DEBUG("Mariana fill buffer with #tuples={} of size={},getBufferSize={} ", generatedTuplesThisPass, tupleSize, buffer.getBuffer().getBufferSize());
        NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < buffer.getBuffer().getBufferSize(), "Wrong parameters");
    }
    NES_TRACE("CSVSource::fillBuffer: fill buffer with #tuples={} of size={}", generatedTuplesThisPass, tupleSize);

    std::string line;
    uint64_t tupleCount = 0;

    if (skipHeader && currentPositionInFile == 0) {
        NES_TRACE("CSVSource: Skipping header");
        std::getline(input, line);
        currentPositionInFile = input.tellg();
    }

    while (tupleCount < generatedTuplesThisPass) {

        //Check if EOF has reached
        if (auto const tg = input.tellg(); (tg >= 0 && static_cast<uint64_t>(tg) >= fileSize) || tg == -1) {
            NES_TRACE("CSVSource::fillBuffer: reset tellg()={} fileSize={}", input.tellg(), fileSize);
            input.clear();
            NES_TRACE("CSVSource::fillBuffer: break because file ended");
            this->fileEnded = true;
            break;
        }

        std::getline(input, line);
        NES_TRACE("CSVSource line={} val={}", tupleCount, line);
        // TODO: there will be a problem with non-printable characters (at least with null terminators). Check sources

        inputParser->writeInputTupleToTupleBuffer(line, tupleCount, buffer, schema, localBufferManager);
        tupleCount++;
    }//end of while

    currentPositionInFile = input.tellg();
    buffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    NES_TRACE("CSVSource::fillBuffer: reading finished read {} tuples at posInFile={}", tupleCount, currentPositionInFile);
    NES_TRACE("CSVSource::fillBuffer: read produced buffer=  {}", Util::printTupleBufferAsCSV(buffer.getBuffer(), schema));
}

SourceType CSVSource::getType() const { return SourceType::CSV_SOURCE; }

std::string CSVSource::getFilePath() const { return filePath; }

const CSVSourceTypePtr& CSVSource::getSourceConfig() const { return csvSourceType; }
}// namespace NES
