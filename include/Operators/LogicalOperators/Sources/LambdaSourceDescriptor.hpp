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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_LambdaSourceDescriptor_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_LambdaSourceDescriptor_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <functional>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES {
/**
 * @brief Descriptor defining properties used for creating physical memory source
 */
class LambdaSourceDescriptor : public SourceDescriptor {
  public:
    /**
     * @brief Ctor of a LambdaSourceDescriptor
     * @param schema the schame of the source
     * @param lambda function
     */
    explicit LambdaSourceDescriptor(
        SchemaPtr schema,
        const std::function<void(NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>& generationFunction,
        uint64_t numBuffersToProcess, std::chrono::milliseconds frequency);

    /**
     * @brief Factory method to create a LambdaSourceDescriptor object
     * @param schema the schema of the source
     * @param lambda function
     * @param numBuffersToProcess
     * @param frequency
     * @return a correctly initialized shared ptr to LambdaSourceDescriptor
     */
    static std::shared_ptr<LambdaSourceDescriptor>
    create(SchemaPtr schema,
           const std::function<void(NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>& generationFunction,
           uint64_t numBuffersToProcess, std::chrono::milliseconds frequency);

    /**
     * @brief Provides the string representation of the memory source
     * @return the string representation of the memory source
     */
    std::string toString() override;

    /**
     * @brief Equality method to compare two source descriptors stored as shared_ptr
     * @param other the source descriptor to compare against
     * @return true if type, schema, and memory area are equal
     */
    bool equal(SourceDescriptorPtr other) override;

    /**
     * @brief returns the the generator function
     * @return generator function
     */
    const std::function<void(NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>& getGeneratorFunction();

    uint64_t getNumBuffersToProcess() const;
    std::chrono::milliseconds getFrequency() const;

  private:
    const std::function<void(NES::NodeEngine::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>& generationFunction;
    uint64_t numBuffersToProcess;
    std::chrono::milliseconds frequency;
};
}// namespace NES

#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_LambdaSourceDescriptor_HPP_
