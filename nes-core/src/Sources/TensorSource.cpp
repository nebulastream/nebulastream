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
#include <Sources/TensorSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace NES {

TensorSource::TensorSource(SchemaPtr schema,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     TensorSourceTypePtr tensorSourceType,
                     OperatorId operatorId,
                     OriginId originId,
                     size_t numSourceLocalBuffers,
                     GatheringMode::Value gatheringMode,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(successors)),
      tensorSourceType(tensorSourceType) {
    this->tupleSize = schema->getSchemaSizeInBytes();

    struct Deleter {
        void operator()(const char* ptr) { std::free(const_cast<char*>(ptr)); }
    };

    auto path = std::unique_ptr<const char, Deleter>(const_cast<const char*>(realpath(tensorSourceType->getFilePath()->getValue().c_str(), nullptr)));
    NES_DEBUG("TensorSource: Opening path=[" << tensorSourceType->getFilePath()->getValue() << "] real path=[" << (path ? path.get() : "<INVALID>") << "]");

    if (path == nullptr) {
        NES_THROW_RUNTIME_ERROR("Could not determine absolute pathname: " << tensorSourceType->getFilePath()->getValue());
    }

    input.open(path.get());
    if (!(input.is_open() && input.good())) {
        throw Exceptions::RuntimeException("Cannot open file: " + std::string(path.get()));
    }
    NES_DEBUG("TensorSource: Opening path " << path.get());
    input.seekg(0, std::ifstream::end);
    if (auto const reportedFileSize = input.tellg(); reportedFileSize == -1) {
        throw Exceptions::RuntimeException("TensorSource::TensorSource File " + tensorSourceType->getFilePath()->getValue() + " is corrupted");
    } else {
        this->fileSize = static_cast<decltype(this->fileSize)>(reportedFileSize);
    }

    this->loopOnFile = tensorSourceType->getNumberOfBuffersToProduce()->getValue() == 0;

    NES_DEBUG("TensorSource: tupleSize=" << this->tupleSize << " freq=" << this->gatheringInterval.count() << "ms"
                                      << " numBuff=" << this->tensorSourceType->getNumberOfBuffersToProduce()->getValue() << " numberOfTuplesToProducePerBuffer="
                                      << this->tensorSourceType->getNumberOfTuplesToProducePerBuffer()->getValue() << "loopOnFile=" << this->loopOnFile);

    this->fileEnded = false;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    this->inputParser = std::make_shared<CSVParser>(schema->getSize(), physicalTypes, this->tensorSourceType->getDelimiter()->getValue());
}

std::optional<Runtime::TupleBuffer> TensorSource::receiveData() {
    NES_TRACE("TensorSource::receiveData called on " << operatorId);
    auto buffer = allocateBuffer();
    fillBuffer(buffer);
    NES_TRACE("TensorSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer.getBuffer();
}

std::string TensorSource::toString() const {
    std::stringstream ss;
    ss << "TENSOR_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << tensorSourceType->getFilePath()->getValue() << " freq=" << this->gatheringInterval.count()
       << "ms"
       << " numBuff=" << this->numBuffersToProcess << ")";
    return ss.str();
}

void TensorSource::fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    NES_TRACE("TensorSource::fillBuffer: start at pos=" << currentPositionInFile << " fileSize=" << fileSize);
    if (this->fileEnded) {
        NES_WARNING("TensorSource::fillBuffer: but file has already ended");
        buffer.setNumberOfTuples(0);
        return;
    }
    input.seekg(currentPositionInFile, std::ifstream::beg);

    uint64_t generatedTuplesThisPass = 0;
    //fill buffer maximally
    if (tensorSourceType->getNumberOfBuffersToProduce()->getValue() == 0) {
        generatedTuplesThisPass = buffer.getCapacity();
    } else {
        generatedTuplesThisPass = tensorSourceType->getNumberOfTuplesToProducePerBuffer()->getValue();
        NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < buffer.getBuffer().getBufferSize(), "Wrong parameters");
    }
    NES_TRACE("TensorSource::fillBuffer: fill buffer with #tuples=" << generatedTuplesThisPass << " of size=" << tupleSize);

    std::string line;
    uint64_t tupleCount = 0;

    if (tensorSourceType->getSkipHeader()->getValue() && currentPositionInFile == 0) {
        NES_TRACE("TensorSource: Skipping header");
        std::getline(input, line);
        currentPositionInFile = input.tellg();
    }

    while (tupleCount < generatedTuplesThisPass) {
        if (auto const tg = input.tellg(); (tg >= 0 && static_cast<uint64_t>(tg) >= fileSize) || tg == -1) {
            NES_TRACE("TensorSource::fillBuffer: reset tellg()=" << input.tellg() << " fileSize=" << fileSize);
            input.clear();
            input.seekg(0, std::ifstream::beg);
            if (!this->loopOnFile) {
                NES_TRACE("TensorSource::fillBuffer: break because file ended");
                this->fileEnded = true;
                break;
            }
            if (tensorSourceType->getSkipHeader()->getValue()) {
                NES_TRACE("TensorSource: Skipping header");
                std::getline(input, line);
                currentPositionInFile = input.tellg();
            }
        }

        std::getline(input, line);
        NES_TRACE("TensorSource line=" << tupleCount << " val=" << line);
        // TODO: there will be a problem with non-printable characters (at least with null terminators). Check sources

        inputParser->writeInputTupleToTupleBuffer(line, tupleCount, buffer, schema);
        tupleCount++;
    }//end of while

    currentPositionInFile = input.tellg();
    buffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    NES_TRACE("TensorSource::fillBuffer: reading finished read " << tupleCount << " tuples at posInFile=" << currentPositionInFile);
    NES_TRACE("TensorSource::fillBuffer: read produced buffer= " << Util::printTupleBufferAsCSV(buffer.getBuffer(), schema));
}

SourceType TensorSource::getType() const { return TENSOR_SOURCE; }

const TensorSourceTypePtr& TensorSource::getSourceConfig() const { return tensorSourceType; }
}// namespace NES
