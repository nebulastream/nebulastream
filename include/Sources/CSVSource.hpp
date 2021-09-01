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

#ifndef INCLUDE_CSVSOURCE_H_
#define INCLUDE_CSVSOURCE_H_

#include <Sources/DataSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <chrono>
#include <fstream>
#include <string>

namespace NES {

/**
 * @brief this class implement the CSV as an input source
 */
class CSVSource : public DataSource {
  public:
    /**
   * @brief constructor of CSV sou1rce
   * @param schema of the source
   * @param path to the csv file
   * @param delimiter inside the file, default ","
   * @param number of buffers to create
   */
    explicit CSVSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       std::string const& filePath,
                       std::string const& delimiter,
                       uint64_t numberOfTuplesToProducePerBuffer,
                       uint64_t numBuffersToProcess,
                       uint64_t frequency,
                       bool skipHeader,
                       OperatorId operatorId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
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
    void fillBuffer(Runtime::TupleBuffer&);

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

  protected:
    std::ifstream input;
    bool fileEnded;
    bool loopOnFile;

  private:
    std::string filePath;
    uint64_t tupleSize;
    uint64_t numberOfTuplesToProducePerBuffer;
    std::string delimiter;
    uint64_t currentPositionInFile{0};
    std::vector<PhysicalTypePtr> physicalTypes;
    size_t fileSize;
    bool skipHeader;
    std::unique_ptr<CSVParser> inputParser;
};

using CSVSourcePtr = std::shared_ptr<CSVSource>;
}// namespace NES

#endif /* INCLUDE_CSVSOURCE_H_ */
