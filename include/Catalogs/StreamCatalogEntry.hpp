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
#include <Catalogs/PhysicalStreamState.hpp>
#include <memory>
#include <sstream>
#include <string>

namespace NES {

class TopologyNode;
typedef std::shared_ptr<TopologyNode> TopologyNodePtr;

class StreamCatalogEntry;
typedef std::shared_ptr<StreamCatalogEntry> StreamCatalogEntryPtr;

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
    /**
     * @brief Create the shared pointer for the stream catalog entry
     * @param sourceType: the source type
     * @param physicalStreamName: physical stream name
     * @param logicalStreamName: a vector of logical stream names
     * @param node: the topology node
     * @return shared pointer to stream catalog
     */
    static StreamCatalogEntryPtr
    create(std::string sourceType, std::string physicalStreamName, std::vector<std::string> logicalStreamName, TopologyNodePtr node);

    /**
     * @brief Create the shared pointer for the stream catalog entry
     * @param config : the physical stream config
     * @param node : the topology node
     * @return shared pointer to stream catalog
     */
    static StreamCatalogEntryPtr create(AbstractPhysicalStreamConfigPtr config, TopologyNodePtr node);

    explicit StreamCatalogEntry(std::string sourceType,
                                std::string physicalStreamName,
                                std::vector<std::string> logicalStreamName,
                                TopologyNodePtr node);

    explicit StreamCatalogEntry(AbstractPhysicalStreamConfigPtr config, TopologyNodePtr node);

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
     * @brief get a vector of logical stream names
     * @return vector of names as strings
     */
    std::vector<std::string> getLogicalName();

    /**
     * @brief get a vector of logical stream names which are mismapped (logical schema does not exists)
     * @return vector of names as strings
     */
    std::vector<std::string> getMissmappedLogicalName();

    /**
     * @brief add a logical stream name
     * @return true if the logicalStreamName was added successfully, false otherwise
     */
     bool addLogicalStreamName(std::string newLogicalStreamName);

     /**
      * @brief add a logical stream name to mismapped (logical streams without schema)
      * @return true if the logicalStreamName was added successfully, false otherwise
      */
      bool addLogicalStreamNameToMismapped(std::string newLogicalStreamName);

    /**
    * @brief change a formerly mismapped logical stream to a regular one
    * @return true if the logicalStreamName was added successfully, false otherwise
    */
      void moveLogicalStreamFromMismappedToRegular(std::string newLogicalStreamName);

    /**
    * @brief remove a logical stream from the vector of mismapped logical streams
    * @return true if the logicalStreamName was removed successfully, false otherwise
    */
      bool removeLogicalStreamFromMismapped(std::string oldLogicalStreamName);

    /**
    * @brief remove a logical stream from the vector of regular logical streams
    * @return true if the logicalStreamName was removed successfully, false otherwise
    */
      bool removeLogicalStream(std::string oldLogicalStreamName);

      /**
       * @brief changes the state to having no logical stream (method should only be called from within this class
       * or through StreamCatalog::addPhysicalStreamWithoutLogicalStreams)
       */
      void setStateToWithoutLogicalStream();

    /**
     * @brief get PhysicalStreamState which contains count (#logicalstreams for that physicalStream) and state (e.g. misconfigured, regular)
     * @return PhysicalStreamState object
     */
    PhysicalStreamState getPhysicalStreamState();

    std::string toString();

  private:
    std::string sourceType;
    std::string physicalStreamName;
    std::vector<std::string> logicalStreamName;
    std::vector<std::string> mismappedLogicalStreamName;
    TopologyNodePtr node;
    PhysicalStreamState physicalStreamState;
};

}// namespace NES

#endif /* INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_ */
