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
#ifndef INCLUDE_HDFSSOURCE_H_
#define INCLUDE_HDFSSOURCE_H_

#include <Sources/DataSource.hpp>
#include <fstream>
#include <string>
#include <HDFS/hdfs.h>

namespace NES {

/**
 * @brief this class implement the HDFS as an input source
 */
class HdfsSource : public DataSource {
  public:
    /**
   * @brief constructor of HDFS source
   * @param schema of the source
   * @param bufferManager
   * @param queryManager
   * @param HDFS namenode
   * @param HDFS port
   * @param path to the csv file
   * @param delimiter inside the file, default ","
   * @param number of tuples per buffer
   * @param number of buffers to create
   * @param frequency
   * @param skip header or not
   * @param operatorId
   */
    explicit HdfsSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                        const std::string namenode, uint64_t port, const std::string filePath, const std::string delimiter,
                        uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess, uint64_t frequency,
                        bool skipHeader, OperatorId operatorId);

    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    std::optional<NodeEngine::TupleBuffer> receiveData() override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    void fillBuffer(NodeEngine::TupleBuffer&);

    /**
     * @brief override the toString method for the csv source
     * @return returns string describing the binary source
     */
    const std::string toString() const override;



    /**
     * @brief Get source type
     * @return source type
     */
    SourceType getType() const override;

    /**
     * @brief Get HDFS namenode
     */
    const std::string getNamenode() const;

    /**
     * @brief Get the HDFS port
     */
    const uint64_t getPort() const;

    /**
     * @brief Get file path for the csv file
     */
    const std::string getFilePath() const;

    /**
     * @brief Get the csv file delimiter
     */
    const std::string getDelimiter() const;

    /**
     * @brief Get number of tuples per buffer
     */
    const uint64_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief getter for skip header
     */
    bool getSkipHeader() const;

    /**
     * @brief getter for hdfsBuilder
     * @return hdfsBuilder
     */
    struct hdfsBuilder *getBuilder();

  private:
    // TODO: Here add fs, hdfsfile
    // TODO: Remove skipheader
    std::string namenode;
    uint64_t port;
    std::string filePath;
    uint64_t tupleSize;
    uint64_t numberOfTuplesToProducePerBuffer;
    std::string delimiter;
    uint64_t currentPosInFile;
    bool loopOnFile;
    std::ifstream input;
    uint64_t fileSize;
    bool fileEnded;
    bool skipHeader;
    hdfsFS fs;
    hdfsStreamBuilder *streamBuilder;
    hdfsFile file;
    hdfsFileInfo *fileInfo;
    struct hdfsBuilder *builder;
};

typedef std::shared_ptr<HdfsSource> HdfsSourcePtr;
}// namespace NES

#endif /* INCLUDE_CSVSOURCE_H_ */


#ifndef NES_HDFSSOURCE_HPP
#define NES_HDFSSOURCE_HPP

class HdfsSource {};

#endif//NES_HDFSSOURCE_HPP
