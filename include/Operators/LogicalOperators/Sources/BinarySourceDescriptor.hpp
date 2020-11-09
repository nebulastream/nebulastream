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

#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical binary source
 */
class BinarySourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string filePath, size_t sourceId);
    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, std::string filePath, size_t sourceId);

    /**
     * @brief Get the path of binary file
     * @return
     */
    const std::string& getFilePath() const;

    bool equal(SourceDescriptorPtr other) override;

    std::string toString() override;

  private:
    explicit BinarySourceDescriptor(SchemaPtr schema, std::string filePath, size_t sourceId);
    explicit BinarySourceDescriptor(SchemaPtr schema, std::string streamName, std::string filePath, size_t sourceId);

    std::string filePath;
};

typedef std::shared_ptr<BinarySourceDescriptor> BinarySourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_
