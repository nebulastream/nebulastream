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

#ifndef INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_
#define INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <memory>
#include <sstream>
#include <string>

namespace NES {

class TopologyNode;
typedef std::shared_ptr<TopologyNode> TopologyNodePtr;

/**
 * @brief one entry in the catalog contains
 *    - the dataSource that can be created there
 *    - the entry in the topology that offer this stream
 *    - the name of the physical stream
 * @caution combination of node and name has to be unique
 * @Limitations
 *
 */
class StreamCatalogEntry {

  public:
    StreamCatalogEntry(PhysicalStreamConfigPtr streamConf, TopologyNodePtr node);

    /**
     * @brief get source type
     * @return type as string
     */
    std::string getSourceType();

    /**
     * @brief get source config
     * @return config as string
     */
    std::string getSourceConfig();

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

    /**
     * @brief get source fequence
     * @return frequency as double
     */
    double getSourceFrequency();

    /**
     * @brief get number of tuples per buffer
     * @return tuple cnt
     */
    size_t getNumberOfTuplesToProducePerBuffer();

    /**
     * @brief get number of buffers to produce
     * @return buffer cnt
     */
    size_t getNumberOfBuffersToProduce();

    std::string toString();

  private:
    PhysicalStreamConfigPtr streamConf;
    TopologyNodePtr node;
};
typedef std::shared_ptr<StreamCatalogEntry> StreamCatalogEntryPtr;

}// namespace NES

#endif /* INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_ */
