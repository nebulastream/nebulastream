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

#ifndef NES_NES_CORE_INCLUDE_SOURCES_PARQUETSOURCE_HPP_
#define NES_NES_CORE_INCLUDE_SOURCES_PARQUETSOURCE_HPP_
#ifdef ENABLE_PARQUET_BUILD
#include <Catalogs/Source/PhysicalSourceTypes/ParquetSourceType.hpp>
#include <Sources/DataSource.hpp>
#include <arrow/io/file.h>
#include <parquet/stream_reader.h>
#include <Sources/Parsers/ParquetParser.hpp>

namespace NES {

/**
 * @brief this class implement the Parquet as an input source
 */
class ParquetSource : public DataSource {
  public:
    /**
   * @brief constructor of Parquet source
   * @param schema of the source
   * @param bufferManager the buffer manager
   * @param queryManager the query manager
   * @param ParquetSourceType points to the current source configuration object, look at mqttSourceType and CSVSourceType for info
   * @param operatorId current operator id
   * @param numSourceLocalBuffers
   * @param successors
   */
    explicit ParquetSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       ParquetSourceTypePtr ParquetSourceType,
                       OperatorId operatorId,
                       OriginId originId,
                       size_t numSourceLocalBuffers,
                       GatheringMode::Value gatheringMode,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer&);

    /**
     * @brief override the toString method for the parquet source
     * @return returns string describing the binary source
     */
    std::string toString() const override;

    /**
     * @brief Get source type
     * @return source type
     */
    SourceType getType() const override;

    /**
     * @brief Get file path for the csv file
     */
    std::string getFilePath() const;

    /**
     * @brief Get number of tuples per buffer
     */
    uint64_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief getter for source config
     * @return ParquetSourceType1
     */
    const ParquetSourceTypePtr& getSourceConfig() const;

  private:
    ParquetSourceTypePtr parquetSourceType;
    uint64_t tupleSize;
    uint64_t numberOfTuplesToProducePerBuffer;
    std::vector<PhysicalTypePtr> physicalTypes;
    std::string filePath;
    ParquetParserPtr parquetParser;

};

using ParquetSourcePtr = std::shared_ptr<ParquetSource>;
}//namespace NES
#endif//NES_NES_CORE_INCLUDE_SOURCES_PARQUETSOURCE_HPP_
#endif

