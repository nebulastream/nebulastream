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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_LAMBDASOURCEDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_LAMBDASOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Util/GatheringMode.hpp>
#include <functional>

namespace NES {

namespace Runtime {
class TupleBuffer;
}

/**
 * @brief Descriptor defining properties used for creating physical memory source
 */
class LambdaSourceDescriptor : public SourceDescriptor {
  public:
    /**
     * @brief Constructor for a LambdaSourceDescriptor
     * @param schema the schema of the source
     * @param lambda function
     * @param numBuffersToProduce number of buffers to produce
     * @param gatheringValue gathering value
     * @param gatheringMode gathering mode
     * @param sourceAffinity sourceAffinity
     * @param taskQueueId taskQueueId
     */
    explicit LambdaSourceDescriptor(
        SchemaPtr schema,
        std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
        uint64_t numBuffersToProduce,
        uint64_t gatheringValue,
        GatheringMode gatheringMode,
        uint64_t sourceAffinity,
        uint64_t taskQueueId,
        std::string logicalSourceName,
        std::string physicalSourceName);

    /**
     * @brief Factory method to create a LambdaSourceDescriptor object
     * @param schema the schema of the source
     * @param lambda function
     * @param numBuffersToProcess
     * @param frequency
     * @return a correctly initialized shared ptr to LambdaSourceDescriptor
     */
    static std::shared_ptr<LambdaSourceDescriptor>
    create(const SchemaPtr& schema,
           std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
           uint64_t numBuffersToProcess,
           uint64_t gatheringValue,
           GatheringMode gatheringMode,
           uint64_t sourceAffinity = 0,
           uint64_t taskQueueId = 0,
           std::string logicalSourceName = "",
           std::string physicalSourceName = "");

    /**
     * @brief Provides the string representation of the memory source
     * @return the string representation of the memory source
     */
    std::string toString() const override;

    /**
     * @brief Equality method to compare two source descriptors stored as shared_ptr
     * @param other the source descriptor to compare against
     * @return true if type, schema, and memory area are equal
     */
    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) const override;

    /**
     * @brief returns the the generator function
     * @return generator function
     */
    std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& getGeneratorFunction();

    /**
     * @brief returns number of buffer to process
     * @return
     */
    uint64_t getNumBuffersToProcess() const;

    /**
     * @brief return the gathering mode
     * @return
     */
    GatheringMode getGatheringMode() const;

    /**
     * @brief return the gathering value
     * @return
     */
    uint64_t getGatheringValue() const;

    /**
    * @brief return the source affinity thus on which core this source is mapped
    * @return
    */
    uint64_t getSourceAffinity() const;

    SourceDescriptorPtr copy() override;

    /**
    * @brief return the task queue id thus on which core this source is mapped
    * @return
    */
    uint64_t getTaskQueueId() const;

  private:
    std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)> generationFunction;
    uint64_t numBuffersToProcess;
    uint64_t gatheringValue;
    GatheringMode gatheringMode;
    uint64_t sourceAffinity;
    uint64_t taskQueueId;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_LAMBDASOURCEDESCRIPTOR_HPP_
