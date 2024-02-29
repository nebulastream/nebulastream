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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_TCPSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_TCPSOURCE_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Sources/DataSource.hpp>
#include <Util/CircularBuffer.hpp>
#include <Util/MMapCircularBuffer.hpp>

namespace NES {

class Parser;
using ParserPtr = std::shared_ptr<Parser>;

/**
 * @brief source to receive data via TCP connection
 */
class TCPSource : public DataSource {

  public:
    /**
     * @brief constructor of a TCP Source
     * @param schema the schema of the data
     * @param bufferManager The BufferManager is responsible for: 1. Pooled Buffers: preallocated fixed-size buffers of memory that
     * must be reference counted 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly.
     * They are also subject to reference counting.
     * @param queryManager comes with functionality to manage the queries
     * @param tcpSourceType points at current TCPSourceType config object, look at same named file for info
     * @param operatorId represents a locally running query execution plan
     * @param originId represents an origin
     * @param statisticId represents the unique identifier of components that we can track statistics for
     * @param numSourceLocalBuffers number of local source buffers
     * @param gatheringMode the gathering mode used
     * @param physicalSourceName the name and unique identifier of a physical source
     * @param executableSuccessors executable operators coming after this source
     */
    explicit TCPSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       TCPSourceTypePtr tcpSourceType,
                       OperatorId operatorId,
                       OriginId originId,
                       StatisticId statisticId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       const std::string& physicalSourceName,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors);

    /**
     * @brief override the receiveData method for the csv source
     * @return returns a buffer if available
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    bool fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer&);

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
     * @brief getter for source config
     * @return tcpSourceType
     */
    const TCPSourceTypePtr& getSourceConfig() const;

    /**
     * @brief opens TCP connection
     */
    void open() override;

    /**
     * @brief closes TCP connection
     */
    void close() override;

  private:
    /**
     * \brief converts buffersize in either binary (NES Format) or ASCII (Json and CSV)
     * \param data data memory segment which contains the buffersize
     * \return buffersize
     */
    [[nodiscard]] size_t parseBufferSize(std::span<const char> data) const;

    std::vector<PhysicalTypePtr> physicalTypes;
    ParserPtr inputParser;
    int connection = -1;
    uint64_t tupleSize;
    uint64_t tuplesThisPass;
    TCPSourceTypePtr sourceConfig;
    int sockfd = -1;
    MMapCircularBuffer circularBuffer;
};
using TCPSourcePtr = std::shared_ptr<TCPSource>;
}// namespace NES
#endif// NES_RUNTIME_INCLUDE_SOURCES_TCPSOURCE_HPP_
