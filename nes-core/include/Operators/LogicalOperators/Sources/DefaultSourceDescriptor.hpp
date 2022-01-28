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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <chrono>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical default source
 */
class DefaultSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, uint64_t numbersOfBufferToProduce, uint64_t frequency);
    static SourceDescriptorPtr
    create(SchemaPtr schema, std::string streamName, uint64_t numbersOfBufferToProduce, uint64_t frequency);

    /**
     * @brief Get number of buffers to be produced
     */
    uint64_t getNumbersOfBufferToProduce() const;

    /**
     * @brief Get the frequency to produce the buffers
     */
    std::chrono::milliseconds getFrequency() const;

    /**
     * @brief Get the frequency as number of times units
     */
    uint64_t getFrequencyCount() const;

    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) override;
    std::string toString() override;

    SourceDescriptorPtr copy() override;

  private:
    explicit DefaultSourceDescriptor(SchemaPtr schema, uint64_t numbersOfBufferToProduce, uint64_t frequency);
    explicit DefaultSourceDescriptor(SchemaPtr schema,
                                     std::string streamName,
                                     uint64_t numbersOfBufferToProduce,
                                     uint64_t frequency);
    const uint64_t numbersOfBufferToProduce;
    const std::chrono::milliseconds frequency;
};

using DefaultSourceDescriptorPtr = std::shared_ptr<DefaultSourceDescriptor>;

}// namespace NES

#endif  // NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_DEFAULTSOURCEDESCRIPTOR_HPP_
