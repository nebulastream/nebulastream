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
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors,
                       SourceDescriptor::InputFormat inputFormat);

    ~TCPSource() override;

    std::optional<Runtime::TupleBuffer> receiveData() override;

    bool fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& tupleBuffer);

    std::string toString() const override;

  private:

    TCPSource() = delete;

    friend class DataSource;
    std::vector<PhysicalTypePtr> physicalTypes;
    std::unique_ptr<Parser> inputParser;
    TCPSourceTypePtr sourceConfig;

};
using TCPSourcePtr = std::shared_ptr<TCPSource>;
}
#endif//NES_INCLUDE_SOURCES_TCPSOURCE_HPP
