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
#ifdef ENABLE_PARQUET_BUILD
#include <Sources/ParquetSource.hpp>
#include <Sources/ParquetStaticVariables.hpp>
namespace NES {
ParquetSource::ParquetSource(SchemaPtr schema,
                                  Runtime::BufferManagerPtr bufferManager,
                                  Runtime::QueryManagerPtr queryManager,
                                  ParquetSourceTypePtr parquetSourceType,
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
      parquetSourceType(parquetSourceType),
      numberOfTuplesToProducePerBuffer(parquetSourceType->getNumberOfTuplesToProducePerBuffer()->getValue()),
      filePath(parquetSourceType->getFilePath()->getValue()) {
    this->numBuffersToProcess = parquetSourceType->getNumberOfBuffersToProduce()->getValue();
    this->tupleSize = schema->getSchemaSizeInBytes();

    std::shared_ptr<arrow::io::ReadableFile> infile;

    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filePath));

    auto reader = parquet::StreamReader(parquet::ParquetFileReader::Open(infile));

    parquetParser = std::make_shared<ParquetParser>(physicalTypes, std::move(reader), schema);
}
std::optional<NES::Runtime::TupleBuffer> NES::ParquetSource::receiveData() {
    NES_TRACE("CSVSource::receiveData called on " << operatorId);
    auto buffer = allocateBuffer();
    fillBuffer(buffer);
    NES_TRACE("CSVSource::receiveData filled buffer with tuples=" << buffer.getNumberOfTuples());

    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer.getBuffer();
}

void NES::ParquetSource::fillBuffer(NES::Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    uint64_t tuplesToGenerate = 0;
    //fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0) {
        tuplesToGenerate = buffer.getCapacity();
    } else {
        tuplesToGenerate = numberOfTuplesToProducePerBuffer;
        NES_ASSERT2_FMT(tuplesToGenerate * tupleSize < buffer.getBuffer().getBufferSize(), "Wrong parameters");
    }

    uint64_t generatedTuples = 0;
    while (generatedTuples < tuplesToGenerate) {
       bool success = parquetParser->writeToTupleBuffer(generatedTuples,buffer);
       if(success){
           generatedTuples++;
           buffer.setNumberOfTuples(generatedTuples);
       }
       else {
           break;
       }
    }
}

std::string NES::ParquetSource::toString() const { return std::string("TODO"); };

NES::SourceType NES::ParquetSource::getType() const { return NES::PARQUET_SOURCE; }

std::string NES::ParquetSource::getFilePath() const { return filePath; }

uint64_t NES::ParquetSource::getNumberOfTuplesToProducePerBuffer() const { return numberOfTuplesToProducePerBuffer; }

const NES::ParquetSourceTypePtr& NES::ParquetSource::getSourceConfig() const { return parquetSourceType; }
}//namespace NES
#endif