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

#ifndef NES_CORE_INCLUDE_CATALOGS_SOURCE_SOURCECATALOG_HPP_
#define NES_CORE_INCLUDE_CATALOGS_SOURCE_SOURCECATALOG_HPP_

#include <API/Schema.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <deque>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace NES {

class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

namespace Catalogs::Source {

/**
 * @brief the source catalog handles the mapping of logical to physical sources
 * @Limitations
 *    - TODO: do we really want to identify sources by name or would it be better to introduce an id?
 *    - TODO: add mutex to make it secure
 *    - TODO: delete methods only delete catalog entries not the entries in the topology
 */
class SourceCatalog {
  public:
    /**
   * @brief method to add a logical source
   * @param logical source name
   * @param schema of logical source as object
   * TODO: what to do if logical source exists but the new one has a different schema
   * @return bool indicating if insert was successful
   */
    bool addLogicalSource(const std::string& logicalSourceName, SchemaPtr schemaPtr);

    /**
       * @brief method to delete a logical source
       * @caution this method only remove the entry from the catalog not from the topology
       * @param name of logical source to delete
       * @param bool indicating the success of the removal
       */
    bool removeLogicalSource(const std::string& logicalSourceName);

    /**
   * @brief method to add a physical source
   * @caution combination of node and name has to be unique
   * @return bool indicating success of insert source
   */
    bool addPhysicalSource(const std::string& logicalSourceName, const SourceCatalogEntryPtr& entry);

    /**
   * @brief method to remove a physical source
   * @caution this will not update the topology
   * @param logical source where this physical source reports to
   * @param structure describing the entry in the catalog
   * @return bool indicating success of remove source
   */
    bool removePhysicalSource(const std::string& logicalSourceName, const std::string& physicalSourceName, std::uint64_t hashId);

    /**
   * @brief method to remove a physical source from all logical sources
   * @param param of the node to be deleted
   * @return bool indicating success of remove source
   */

    SchemaPtr getSchemaForLogicalSource(const std::string& logicalSourceName);

    /**
   * @brief method to return the source for an existing logical source
   * @param name of logical source
   * @return smart pointer to a newly created source
   * @note the source will also contain the schema
   */
    LogicalSourcePtr getLogicalSource(const std::string& logicalSourceName);

    /**
   * @brief method to return the source for an existing logical source or throw exception
   * @param name of logical source
   * @return smart pointer to a newly created source
   * @note the source will also contain the schema
   */
    LogicalSourcePtr getLogicalSourceOrThrowException(const std::string& logicalSourceName);

    /**
   * @brief test if logical source with this name exists in the log to schema mapping
   * @param name of the logical source to test
   * @return bool indicating if source exists
   */
    bool containsLogicalSource(const std::string& logicalSourceName);

    /**
   * @brief test if logical source with this name exists in the log to phy mapping
   * @param name of the logical source to test
   * @return bool indicating if source exists
   */
    bool testIfLogicalSourceExistsInLogicalToPhysicalMapping(const std::string& logicalSourceName);

    /**
   * @brief return all physical nodes that contribute to this logical source
   * @param name of logical source
   * @return list of physical nodes as pointers into the topology
   */
    std::vector<TopologyNodePtr> getSourceNodesForLogicalSource(const std::string& logicalSourceName);

    /**
   * @brief reset the catalog and recreate the default_logical source
   * @return bool indicating success
   */
    bool reset();

    /**
   * @brief Return a list of logical source names registered at catalog
   * @return map containing source name as key and schema object as value
   */
    std::map<std::string, SchemaPtr> getAllLogicalSource();

    std::map<std::string, std::string> getAllLogicalSourceAsString();

    /**
       * @brief method to return the physical source and the associated schemas
       * @return string containing the content of the catalog
       */
    std::string getPhysicalSourceAndSchemaAsString();

    /**
     * @brief get all pyhsical sources for a logical source
     * @param logicalSourceName
     * @return
     */
    std::vector<SourceCatalogEntryPtr> getPhysicalSources(const std::string& logicalSourceName);

    /**
     * @brief method to update a logical source
     * @param logical source name
     * @param schema of logical source as object
     * @return bool indicating if update was successful
     */
    bool updateLogicalSource(const std::string& logicalSourceName, SchemaPtr schemaPtr);

    SourceCatalog();

    /**
     * @brief checks if a source with physicalSourceName exists in the vector of SourceCatalogEntries and if so returns it
     * @param physicalSourceName the unique identifier to a physicalSource
     * @param allSources a vector of source entries
     * @return a SourceCatalogEntryPtr to the source specified by the physicalSourceName
     */
    SourceCatalogEntryPtr getSourceCatalogEntry(const std::string& physicalSourceName,
                                                const std::vector<SourceCatalogEntryPtr>& allSources);

    /**
     * @brief checks if a whole list of physicalSource(Names) is part of a logicalSource and if so returns a vector of the sources
     * @param logicalSourceName the name of the larger logical source
     * @param allPhysicalSourceNames a vector of physicalSourceNames
     * @return all associated SourceCatalogEntryPtrs to the queried physicalSourceNames, but if one physicalSourceName is not
     * part of the logicalSource associated with logicalSourceName, then a vector only holding a nullptr
     */
    std::vector<SourceCatalogEntryPtr> getSubsetOfPhysicalSources(const std::string& logicalSourceName,
                                                                  const std::vector<std::string>& allPhysicalSourceNames);

    /**
     * @brief compare function for two SourceCatalogEntry objects such that they can be sorted
     * @param entry1 a SourceCatalogEntry
     * @param entry2 another SourceCatalogEntry
     * @return returns true if the Node Pointer of entry1 is smaller than that of entry2 and otherwise false
     */
    static bool compareByNode(const SourceCatalogEntryPtr& entry1, const SourceCatalogEntryPtr& entry2);

  private:
    std::recursive_mutex catalogMutex;
    //map logical source to schema
    std::map<std::string, SchemaPtr> logicalSourceNameToSchemaMapping;
    //map logical source to physical source
    std::map<std::string, std::vector<SourceCatalogEntryPtr>> logicalToPhysicalSourceMapping;
    void addDefaultSources();
};
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source
}// namespace NES
#endif// NES_CORE_INCLUDE_CATALOGS_SOURCE_SOURCECATALOG_HPP_
