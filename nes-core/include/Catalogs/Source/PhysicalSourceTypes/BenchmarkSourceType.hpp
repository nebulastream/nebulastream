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

#ifndef NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_BENCHMARKSOURCETYPE_HPP_
#define NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_BENCHMARKSOURCETYPE_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Sources/BenchmarkSource.hpp>

namespace NES {

class BenchmarkSourceType;
using BenchmarkSourceTypePtr = std::shared_ptr<BenchmarkSourceType>;

/**
 * @brief A stream config for a benchm source
 */
class BenchmarkSourceType : public PhysicalSourceType {
  public:
    /**
     * @brief Factory method of BenchmarkSourceType
     * @param memoryArea the pointer to the memory area
     * @param memoryAreaSize the size of the memory area
     * @param numBuffersToProduce the number of buffers to produce
     * @param gatheringValue the gathering value
     * @param gatheringMode the gathering mode
     * @param sourceMode the source mode
     * @param sourceAffinity the source affinity
     * @return a constructed BenchmarkSourceType
     */
    static BenchmarkSourceTypePtr create(uint8_t* memoryArea,
                                         size_t memoryAreaSize,
                                         uint64_t numberOfBuffersToProduce,
                                         uint64_t gatheringValue,
                                         const std::string& gatheringMode,
                                         const std::string& sourceMode,
                                         uint64_t sourceAffinity = std::numeric_limits<uint64_t>::max());

    const std::shared_ptr<uint8_t>& getMemoryArea() const;

    size_t getMemoryAreaSize() const;

    uint64_t getGatheringValue() const;

    GatheringMode::Value getGatheringMode() const;

    SourceMode::Value getSourceMode() const;

    uint64_t getSourceAffinity() const;

    uint64_t getNumberOfBuffersToProduce() const;

    void reset() override;

    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

  private:
    /**
     * @brief Create a BenchmarkSourceType using a set of parameters
     * @param memoryArea the pointer to the memory area
     * @param memoryAreaSize the size of the memory area
     * @param numBuffersToProduce the number of buffers to produce
     * @param gatheringValue the gathering value
     * @param gatheringMode the gathering mode
     * @param sourceMode the source mode
     * @param sourceAffinity the source affinity
     */
    explicit BenchmarkSourceType(uint8_t* memoryArea,
                                 size_t memoryAreaSize,
                                 uint64_t numBuffersToProduce,
                                 uint64_t gatheringValue,
                                 GatheringMode::Value gatheringMode,
                                 SourceMode::Value sourceMode,
                                 uint64_t sourceAffinity);

    std::shared_ptr<uint8_t> memoryArea;
    size_t memoryAreaSize;
    uint64_t numberOfBuffersToProduce;
    uint64_t gatheringValue;
    GatheringMode::Value gatheringMode;
    SourceMode::Value sourceMode;
    uint64_t sourceAffinity;
};
}// namespace NES
#endif  // NES_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_BENCHMARKSOURCETYPE_HPP_
