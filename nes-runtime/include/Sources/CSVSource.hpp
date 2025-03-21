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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_CSVSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_CSVSOURCE_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <chrono>
#include <fstream>
#include <string>
#include <folly/Synchronized.h>

namespace NES {

class CSVParser;
using CSVParserPtr = std::shared_ptr<CSVParser>;
/**
 * @brief this class implement the CSV as an input source
 */
class CSVSource : public DataSource {
  public:
    /**
   * @brief constructor of the CSV source
   * @param schema of the source
   * @param bufferManager pointer to the buffer manager
   * @param queryManager pointer to the query manager
   * @param csvSourceType points to the current source configuration object, look at mqttSourceType and CSVSourceType for info
   * @param operatorId current operator id
   * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
     * @param statisticId represents the unique identifier of components that we can track statistics for
   * @param numSourceLocalBuffers number of local source buffers
   * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
   * @param physicalSourceName the name and unique identifier of a physical source
   * @param successors the subsequent operators in the pipeline to which the data is pushed
   * @return a DataSourcePtr pointing to the data source
   */
    explicit CSVSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       CSVSourceTypePtr csvSourceType,
                       OperatorId operatorId,
                       OriginId originId,
                       StatisticId statisticId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       const std::string& physicalSourceName,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors, bool addTimestampsAndReadOnStartup = true);

    virtual ~CSVSource();

    std::pair<size_t, size_t> findWatermarkIndex(const std::vector<std::vector<Runtime::Record>>& records, uint64_t sentUntil);
    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    void fillBuffer(Runtime::MemoryLayouts::TestTupleBuffer&);

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
     * @brief getter for source config
     * @return csvSourceType1
     */
    const CSVSourceTypePtr& getSourceConfig() const;

  protected:
    std::ifstream input;
    bool fileEnded;

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
    uint64_t sentUntil = 0;
    std::pair<size_t, size_t> watermarkIndex = {0, 0};
    //std::vector<Runtime::MemoryLayouts::DynamicTupleBuffer> readLines;
//    std::vector<std::string> readLines;
//    uint64_t nextLinesIndex = 0;
    bool addTimeStampsAndReadOnStartup;
//    uint64_t port;
//    int sockfd;
//    std::vector<uint8_t> incomingBuffer;
//    std::vector<uint8_t> leftOverBytes;
//    uint16_t leftoverByteCount = 0;
    //uint64_t totalTupleCount = 0;
    std::optional<Runtime::TupleBuffer> fillReplayBuffer(folly::Synchronized<Runtime::TcpSourceInfo>::LockedPtr& sourceInfo, Runtime::MemoryLayouts::TestTupleBuffer& buffer);
};

using CSVSourcePtr = std::shared_ptr<CSVSource>;
}// namespace NES

#endif// NES_RUNTIME_INCLUDE_SOURCES_CSVSOURCE_HPP_
