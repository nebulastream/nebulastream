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

#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical CSV source
 */
class CsvSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string filePath, std::string delimiter, size_t numBuffersToProcess,
                                      size_t numberOfTuplesToProducePerBuffer, size_t frequency, bool endlessRepeat,
                                      bool skipHeader, OperatorId operatorId);

    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, std::string filePath, std::string delimiter,
                                      size_t numberOfTuplesToProducePerBuffer, size_t numBuffersToProcess, size_t frequency,
                                      bool endlessRepeat, bool skipHeader, OperatorId operatorId);

    /**
     * @brief get file path for reading the csv file
     */
    const std::string& getFilePath() const;

    /**
     * @brief get delimiter for the csv file
     */
    const std::string& getDelimiter() const;

    /**
     * @brief Get number of buffers to process
     */
    size_t getNumBuffersToProcess() const;

    /**
    * @brief Get number of number of tuples within the buffer
    */
    size_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief get the frequency of reading the csv file
     */
    size_t getFrequency() const;

    /**
     * @brief get the value of the skipHeader
     */
    bool getSkipHeader() const;

    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

    /**
     * @brief getter/setter endlessRepeat
     * @return
     */
    bool isEndlessRepeat() const;
    void setEndlessRepeat(bool endlessRepeat);

  private:
    explicit CsvSourceDescriptor(SchemaPtr schema, std::string filePath, std::string delimiter,
                                 size_t numberOfTuplesToProducePerBuffer, size_t numBuffersToProcess, size_t frequency,
                                 bool endlessRepeat, bool skipHeader, OperatorId operatorId);

    explicit CsvSourceDescriptor(SchemaPtr schema, std::string streamName, std::string filePath, std::string delimiter,
                                 size_t numberOfTuplesToProducePerBuffer, size_t numBuffersToProcess, size_t frequency,
                                 bool endlessRepeat, bool skipHeader, OperatorId operatorId);

    std::string filePath;
    std::string delimiter;
    size_t numBuffersToProcess;
    size_t numberOfTuplesToProducePerBuffer;
    size_t frequency;
    bool endlessRepeat;
    bool skipHeader;
};

typedef std::shared_ptr<CsvSourceDescriptor> CsvSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_
