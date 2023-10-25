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

#ifndef NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_STATICDATASOURCETYPE_HPP_
#define NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_STATICDATASOURCETYPE_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/GatheringMode.hpp>
#include <Util/SourceMode.hpp>

namespace NES::Experimental {

class StaticDataSourceType;
using StaticDataSourceTypePtr = std::shared_ptr<StaticDataSourceType>;

/**
 * @brief A source config for a static data source
 */
class StaticDataSourceType : public PhysicalSourceType {

  public:
    /**
     * @brief Factory method of StaticDataSourceType
     * @param pathTableFile here, a file in pipe-seperated .tbl file format should exist in the local file system
     * @param numBuffersToProcess
     * @param lateStart indicates if the static data source should start sending data at deployment or only when receiving a "start" message
     * @return a constructed StaticDataSourceType
     */
    static StaticDataSourceTypePtr create(const std::string& logicalSourceName, const std::string& physicalSourceName,const std::string& pathTableFile,
                                          uint64_t numBuffersToProcess,
                                          const std::string& gatheringMode,
                                          uint64_t taskQueueId,
                                          bool lateStart);

    GatheringMode getGatheringMode() const;

    NES::SourceMode getSourceMode() const;

    uint64_t getTaskQueueId() const;

    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    void reset() override;

    std::string getPathTableFile();

    /**
     * @brief Getter for lateStart.
     * @returns lateStart indicates if the static data source should start sending data at deployment or only when receiving a "start" message
     */
    bool getLateStart();

    /**
     * @brief Create a StaticDataSourceType using a set of parameters
     * @param pathTableFile here, a file in pipe-seperated .tbl file format should exist in the local file system
     * @param numBuffersToProcess
     * @param lateStart indicates if the static data source should start sending data at deployment or only when receiving a "start" message
     */
    explicit StaticDataSourceType(const std::string& logicalSourceName, const std::string& physicalSourceName,const std::string& pathTableFile,
                                  uint64_t numBuffersToProcess,
                                  SourceMode sourceMode,
                                  uint64_t taskQueueId,
                                  bool lateStart);

    std::string pathTableFile;
    uint64_t numBuffersToProcess;// todo not used right now [#2493]
    SourceMode sourceMode;
    uint64_t taskQueueId;// todo not used right now [#2493]
    const bool lateStart;
};
}// namespace NES::Experimental
#endif// NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_STATICDATASOURCETYPE_HPP_
