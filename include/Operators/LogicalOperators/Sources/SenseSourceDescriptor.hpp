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

#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SENSESOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SENSESOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical sense source
 */
class SenseSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string udfs, SourceId sourceId);
    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, std::string udfs, SourceId sourceId);

    /**
     * @brief Get the udf for the sense node
     */
    const std::string& getUdfs() const;
    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

  private:
    explicit SenseSourceDescriptor(SchemaPtr schema, std::string udfs, SourceId sourceId);
    explicit SenseSourceDescriptor(SchemaPtr schema, std::string streamName, std::string udfs, SourceId sourceId);

    std::string udfs;
};

typedef std::shared_ptr<SenseSourceDescriptor> SenseSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_SENSESOURCEDESCRIPTOR_HPP_
