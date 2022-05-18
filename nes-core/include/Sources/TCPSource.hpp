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

#ifndef NES_INCLUDE_SOURCES_TCPSOURCE_HPP
#define NES_INCLUDE_SOURCES_TCPSOURCE_HPP

#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/Parsers/Parser.hpp>

namespace NES{

class TupleBuffer;

/**
 * @brief source to recieve data via TCP connection
 */
class TCPSource : public DataSource{

  public:
    explicit TCPSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       const TCPSourceTypePtr& tcpSourceType,
                       OperatorId operatorId,
                       OriginId originId,
                       size_t numSourceLocalBuffers,
                       GatheringMode::Value gatheringMode,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors);

    ~TCPSource() override;

    std::optional<Runtime::TupleBuffer> receiveData() override;

    bool fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer);

    std::string toString() const override;

  private:

    TCPSource() = delete;

    /**
     * @brief method to connect tcp using the host and port specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connected();

    /**
     * @brief method to make sure tcp is disconnected
     * check if already disconnected, if not disconnected try to disconnect, if already disconnected return
     * @return bool indicating if connection could be established
     */
    bool disconnect();

    friend class DataSource;
    std::vector<PhysicalTypePtr> physicalTypes;
    std::unique_ptr<Parser> inputParser;
    int connection;
    uint64_t tupleSize;
    int sock = 0;
    uint64_t tuplesThisPass;
    TCPSourceTypePtr sourceConfig;

};
using TCPSourcePtr = std::shared_ptr<TCPSource>;
}
#endif//NES_INCLUDE_SOURCES_TCPSOURCE_HPP
