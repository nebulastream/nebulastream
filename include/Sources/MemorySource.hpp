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

#ifndef NES_INCLUDE_SOURCES_MEMORYSOURCE_HPP_
#define NES_INCLUDE_SOURCES_MEMORYSOURCE_HPP_

#include <Sources/DataSource.hpp>

namespace NES {

/**
 * @brief Memory Source that reads from main memory and produces buffers.
 * Do not use in distributed settings but only for single node dev and testing.
 */
class MemorySource : public DataSource {
  public:
    explicit MemorySource(SchemaPtr schema, std::shared_ptr<uint8_t> memoryArea, size_t memoryAreaSize,
                          NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                          OperatorId operatorId)
        : DataSource(std::move(schema), std::move(bufferManager), std::move(queryManager), operatorId), memoryArea(memoryArea),
          memoryAreaSize(memoryAreaSize) {
        // nop
    }

    void runningRoutine(NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager) override;

    std::optional<NodeEngine::TupleBuffer> receiveData() override;

    const std::string toString() const override;

    SourceType getType() const override;

  private:
    std::shared_ptr<uint8_t> memoryArea;
    const size_t memoryAreaSize;
};

typedef std::shared_ptr<MemorySource> MemorySourcePtr;

}// namespace NES

#endif//NES_INCLUDE_SOURCES_MEMORYSOURCE_HPP_
