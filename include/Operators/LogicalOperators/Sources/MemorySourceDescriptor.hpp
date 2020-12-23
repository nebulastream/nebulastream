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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MEMORYSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MEMORYSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {
/**
 * @brief Descriptor defining properties used for creating physical memory source
 */
class MemorySourceDescriptor : public SourceDescriptor {
  public:
    explicit MemorySourceDescriptor(SchemaPtr schema, std::shared_ptr<uint8_t> memoryArea, size_t memoryAreaSize);

    static std::shared_ptr<MemorySourceDescriptor> create(SchemaPtr schema, std::shared_ptr<uint8_t> memoryArea, size_t memoryAreaSize);

    std::string toString() override;
    bool equal(SourceDescriptorPtr other) override;

    std::shared_ptr<uint8_t> getMemoryArea() {
        return memoryArea;
    }

    size_t getMemoryAreaSize() const {
        return memoryAreaSize;
    }

  private:
    std::shared_ptr<uint8_t> memoryArea;
    size_t memoryAreaSize;
};
}

#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_MEMORYSOURCEDESCRIPTOR_HPP_
