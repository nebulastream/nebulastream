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
#ifndef NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_LAMBDASOURCETYPE_HPP_
#define NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_LAMBDASOURCETYPE_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/GatheringMode.hpp>
#include <functional>

namespace NES {

namespace Runtime {
class TupleBuffer;
}

class LambdaSourceType;
using LambdaSourceTypePtr = std::shared_ptr<LambdaSourceType>;

/**
 * @brief A source config for a lambda source
 */
class LambdaSourceType : public PhysicalSourceType {
  public:
    /**
     * @brief Factory method of LambdaSourceType
     * @param lambda function that produces the buffer
     * @param numBuffersToProduce The number of buffers, which are produced by this lambda source.
     * @param gatheringValue The gatheringValue
     * @param sourceAffinity: sourceAffinity
     * @param taskQueueId: taskQueueId
     * @return a constructed LambdaSourceType
     */
    static LambdaSourceTypePtr
    create(const std::string& logicalSourceName, const std::string& physicalSourceName,std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
           uint64_t numBuffersToProduce,
           uint64_t gatheringValue,
           GatheringMode gatheringMode,
           uint64_t sourceAffinity = 0,
           uint64_t taskQueueId = 0);

    std::function<void(NES::Runtime::TupleBuffer&, uint64_t)> getGenerationFunction() const;

    uint64_t getNumBuffersToProduce() const;

    uint64_t getGatheringValue() const;

    GatheringMode getGatheringMode() const;

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
    explicit LambdaSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName,
        std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
        uint64_t numBuffersToProduce,
        uint64_t gatheringValue,
        GatheringMode gatheringMode,
        uint64_t sourceAffinity,
        uint64_t taskQueueId);

    std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)> generationFunction;
    uint64_t numBuffersToProduce;
    uint64_t gatheringValue;
    GatheringMode gatheringMode;
    uint64_t sourceAffinity;
    uint64_t taskQueueId;
};

}// namespace NES

#endif// NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_LAMBDASOURCETYPE_HPP_
