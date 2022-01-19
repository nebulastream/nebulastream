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

#ifndef NES_INCLUDE_SOURCES_CSV_SOURCE_HPP_
#define NES_INCLUDE_SOURCES_CSV_SOURCE_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <chrono>
#include <fstream>
#include <string>

namespace NES {

class CSVParser;
using CSVParserPtr = std::shared_ptr<CSVParser>;
/**
 * @brief this class implement the CSV as an input source
 */
class CSVSource : public DataSource {
  public:
    /**
   * @brief constructor of CSV source
   * @param schema of the source
   * @param bufferManager the buffer manager
   * @param queryManager the query manager
   * @param csvSourceType points to the current source configuration object, look at mqttSourceType and CSVSourceType for info
   * @param delimiter inside the file, default ","
   * @param operatorId current operator id
   * @param numSourceLocalBuffers
   * @param gatheringMode
   * @param successors
   */
    explicit CSVSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       CSVSourceTypePtr csvSourceType,
                       OperatorId operatorId,
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
     * @brief override the toString method for the csv source
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
     * @brief Get the csv file delimiter
     */
    std::string getDelimiter() const;

    /**
     * @brief Get number of tuples per buffer
     */
    uint64_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief getter for skip header
     */
    bool getSkipHeader() const;

    /**
     * @brief getter for source config
     * @return csvSourceType
     */
    const CSVSourceTypePtr& getSourceConfig() const;

  protected:
    std::ifstream input;
    bool fileEnded;
    bool loopOnFile;

  private:
    CSVSourceTypePtr csvSourceType;
    std::string filePath;
    uint64_t tupleSize;
    uint64_t numberOfTuplesToProducePerBuffer;
    std::string delimiter;
    uint64_t currentPositionInFile{0};
    std::vector<PhysicalTypePtr> physicalTypes;
    size_t fileSize;
    bool skipHeader;
    CSVParserPtr inputParser;
};

using CSVSourcePtr = std::shared_ptr<CSVSource>;
}// namespace NES

#endif// NES_INCLUDE_SOURCES_CSV_SOURCE_HPP_
