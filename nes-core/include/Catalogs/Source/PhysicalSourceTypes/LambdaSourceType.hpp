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
#ifndef NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_LAMBDASOURCETYPE_HPP_
#define NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_LAMBDASOURCETYPE_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/GatheringMode.hpp>
#include <functional>

namespace NES {

namespace Runtime {
class TupleBuffer;
}

class LambdaSourceType;
using LambdaSourceTypePtr = std::shared_ptr<LambdaSourceType>;

/**
 * @brief A stream config for a lambda source
 */
class LambdaSourceType : public PhysicalSourceType {
  public:
    /**
     * @brief Factory method of LambdaSourceType
     * @param sourceType the type of the source
     * @param physicalStreamName the name of the physical stream
     * @param logicalStreamName the name of the logical stream
     * @param lambda function that produces the buffer
     * @param sourceAffinity: sourceAffinity
     * @param taskQueueId: taskQueueId
     * @return a constructed LambdaSourceType
     */
    static LambdaSourceTypePtr
    create(std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
           uint64_t numBuffersToProduce,
           uint64_t gatheringValue,
           const std::string& gatheringMode,
           uint64_t sourceAffinity = 0,
           uint64_t taskQueueId = 0);

    std::function<void(NES::Runtime::TupleBuffer&, uint64_t)> getGenerationFunction() const;

    uint64_t getNumBuffersToProduce() const;

    uint64_t getGatheringValue() const;

    GatheringMode::Value getGatheringMode() const;

    uint64_t getTaskQueueId() const;

    uint64_t getSourceAffinity() const;

    /**
     * @brief The string representation of the object
     * @return the string representation of the object
     */
    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    void reset() override;

  private:
    explicit LambdaSourceType(
        std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
        uint64_t numBuffersToProduce,
        uint64_t gatheringValue,
        GatheringMode::Value gatheringMode,
        uint64_t sourceAffinity,
        uint64_t taskQueueId);

    std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)> generationFunction;
    uint64_t numBuffersToProduce;
    uint64_t gatheringValue;
    GatheringMode::Value gatheringMode;
    uint64_t sourceAffinity;
    uint64_t taskQueueId;
};

}// namespace NES

#endif  // NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_LAMBDASOURCETYPE_HPP_
