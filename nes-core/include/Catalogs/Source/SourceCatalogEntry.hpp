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

#ifndef NES_INCLUDE_CATALOGS_STREAM_CATALOG_ENTRY_HPP_
#define NES_INCLUDE_CATALOGS_STREAM_CATALOG_ENTRY_HPP_

#include <memory>
#include <sstream>
#include <string>

namespace NES {

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class SourceCatalogEntry;
using SourceCatalogEntryPtr = std::shared_ptr<SourceCatalogEntry>;

class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

/**
 * @brief one entry in the catalog contains
 *    - the dataSource that can be created there
 *    - the entry in the topology that offer this stream
 *    - the name of the physical stream
 * @caution combination of node and name has to be unique
 * @Limitations
 *
 */
class SourceCatalogEntry {

  public:
    /**
     * @brief Create the shared pointer for the stream catalog entry
     * @param sourceType: the source type
     * @param physicalSourceName: physical stream name
     * @param logicalSourceName: the logical stream name
     * @param node: the topology node
     * @return shared pointer to stream catalog
     */
    static SourceCatalogEntryPtr create(const std::string& sourceType,
                                        const std::string& physicalSourceName,
                                        const std::string& logicalSourceName,
                                        const TopologyNodePtr& node);

    /**
     * @brief Create the shared pointer for the stream catalog entry
     * @param config : the physical stream config
     * @param node : the topology node
     * @return shared pointer to stream catalog
     */
    static SourceCatalogEntryPtr create(const AbstractPhysicalStreamConfigPtr& config, const TopologyNodePtr& node);

    explicit SourceCatalogEntry(std::string sourceType,
                                std::string physicalStreamName,
                                std::string logicalStreamName,
                                TopologyNodePtr node);

    explicit SourceCatalogEntry(const AbstractPhysicalStreamConfigPtr& config, TopologyNodePtr node);

    /**
     * @brief get source type
     * @return type as string
     */
    std::string getSourceType();

    /**
     * @brief get topology pointer
     * @return ptr to node
     */
    TopologyNodePtr getNode();

    /**
     * @brief get physical stream name
     * @return name as string
     */
    std::string getPhysicalName();

    /**
     * @brief get logical stream name
     * @return name as string
     */
    std::string getLogicalName();

    std::string toString();

  private:
    PhysicalSourcePtr physicalSource;

    TopologyNodePtr node;
};

}// namespace NES

#endif// NES_INCLUDE_CATALOGS_STREAM_CATALOG_ENTRY_HPP_
