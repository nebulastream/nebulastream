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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_DEFAULTSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_DEFAULTSOURCE_HPP_
#include <Sources/DataSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <chrono>

namespace NES {

class DefaultSource : public GeneratorSource {
  public:
    /*
   * @brief public constructor for the default source
   * @param schema of the data that this source produces
   * @param bufferManager pointer to the buffer manager
   * @param queryManager pointer to the query manager
   * @param numberOfBuffersToProduce the number of buffers to be produced by the source
   * @param gatheringInterval the interval at which new buffers are produced
   * @param operatorId current operator id
   * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
   * @param numSourceLocalBuffers number of local source buffers
   * @param successors the subsequent operators in the pipeline to which the data is pushed
   * @param logicalSourceName the name of the logicalSource from which we get the data
   * @param physicalSourceName the name and unique identifier of a physical source
   */
    DefaultSource(SchemaPtr schema,
                  Runtime::BufferManagerPtr bufferManager,
                  Runtime::QueryManagerPtr queryManager,
                  uint64_t numberOfBufferToProduce,
                  uint64_t gatheringInterval,
                  OperatorId operatorId,
                  OriginId originId,
                  size_t numSourceLocalBuffers,
                  std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors = {},
                  const std::string& logicalSourceName = std::string("defaultLogicalSourceName"),
                  const std::string& physicalSourceName = std::string("defaultPhysicalSourceName"));

    SourceType getType() const override;

    std::optional<Runtime::TupleBuffer> receiveData() override;
    std::vector<Schema::MemoryLayoutType> getSupportedLayouts() override;
};

using DefaultSourcePtr = std::shared_ptr<DefaultSource>;

}// namespace NES
#endif// NES_RUNTIME_INCLUDE_SOURCES_DEFAULTSOURCE_HPP_
