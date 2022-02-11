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

#ifndef NES_INCLUDE_CATALOGS_TABLE_SOURCE_STREAM_CONFIG_HPP_
#define NES_INCLUDE_CATALOGS_TABLE_SOURCE_STREAM_CONFIG_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Sources/StaticDataSource.hpp>
#include <Util/SourceMode.hpp>

namespace NES::Experimental {

class StaticDataSourceType;
using StaticDataSourceTypePtr = std::shared_ptr<StaticDataSourceType>;

/**
 * @brief A stream config for a static data source
 */
class StaticDataSourceType : public PhysicalSourceType {

public:
    /**
     * @brief Factory method of StaticDataSourceType
      * @param sourceType the type of the source
     * @param physicalStreamName the name of the physical stream
     * @param logicalStreamName the name of the logical stream
     * @param memoryArea the pointer to the memory area
     * @param memoryAreaSize the size of the memory area
     * @return a constructed StaticDataSourceType
     */
    static StaticDataSourceTypePtr create(const std::string& pathTableFile,
                                          uint64_t numBuffersToProcess,
                                          const std::string& gatheringMode,
                                          uint64_t taskQueueId);

    GatheringMode::Value getGatheringMode() const;

    NES::SourceMode::Value getSourceMode() const;

    uint64_t getTaskQueueId() const;

    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    void reset() override;

    std::string getPathTableFile();

    /**
     * @brief Create a StaticDataSourceType using a set of parameters
     * @param sourceType the type of the source
     * @param pathTableFile here, a file in pipe-seperated .tbl file format should exist in the local file system
     * @param numBuffersToProcess
     */
    explicit StaticDataSourceType(const std::string& pathTableFile,
                                  uint64_t numBuffersToProcess,
                                  SourceMode::Value sourceMode,
                                  uint64_t taskQueueId);

    std::string pathTableFile;
    uint64_t numBuffersToProcess;   // todo not used right now [#2493]
    SourceMode::Value sourceMode;
    uint64_t taskQueueId;            // todo not used right now [#2493]
};
}// namespace NES
#endif// NES_INCLUDE_CATALOGS_TABLE_SOURCE_STREAM_CONFIG_HPP_
