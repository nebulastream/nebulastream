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

#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical default source
 */
class DefaultSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, uint64_t numbersOfBufferToProduce, uint32_t frequency);
    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, uint64_t numbersOfBufferToProduce,
                                      uint32_t frequency);

    /**
     * @brief Get number of buffers to be produced
     */
    uint64_t getNumbersOfBufferToProduce() const;

    /**
     * @brief Get the frequency to produce the buffers
     */
    uint32_t getFrequency() const;

    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

  private:
    explicit DefaultSourceDescriptor(SchemaPtr schema, uint64_t numbersOfBufferToProduce, uint32_t frequency,
                                     OperatorId operatorId);
    explicit DefaultSourceDescriptor(SchemaPtr schema, std::string streamName, uint64_t numbersOfBufferToProduce,
                                     uint32_t frequency, OperatorId operatorId);
    const uint64_t numbersOfBufferToProduce;
    const uint32_t frequency;
};

typedef std::shared_ptr<DefaultSourceDescriptor> DefaultSourceDescriptorPtr;

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_
