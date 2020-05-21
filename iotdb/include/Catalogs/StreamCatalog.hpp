#ifndef INCLUDE_CATALOGS_STREAMCATALOG_HPP_
#define INCLUDE_CATALOGS_STREAMCATALOG_HPP_

#include <API/Schema.hpp>
#include <API/Stream.hpp>
#include <SourceSink/DataSource.hpp>
#include <deque>
#include <map>
#include <string>
#include <vector>
//#include <Topology/NESTopologyEntry.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
using namespace std;
namespace NES {

/**
 * @brief the stream catalog handles the mapping of logical to physical streams
 * @Limitations
 *    - TODO: do we really want to identify streams by name or would it be better to introduce an id?
 *    - TODO: add mutex to make it secure
 *    - TODO: delete methods only delete catalog entries not the entries in the topology
 */
class StreamCatalog {
  public:
    /**
   * @brief method to add a logical stream
   * @param logical stream name
   * @param schema of logical stream as object
   * TODO: what to do if logical stream exists but the new one has a different schema
   * @return bool indicating if insert was successful
   */
    bool addLogicalStream(std::string logicalStreamName, SchemaPtr schemaPtr);

    /**
   * @brief method to add a logical stream
   * @param logical stream name
   * @param schema of logical stream as string
   * @return bool indicating if insert was successful
     */
    bool addLogicalStream(const std::string& streamName, const std::string& streamSchema);

    /**
       * @brief method to delete a logical stream
       * @caution this method only remove the entry from the catalog not from the topology
       * @param name of logical stream to delete
       * @param bool indicating the success of the removal
       */
    bool removeLogicalStream(std::string logicalStreamName);

    /**
   * @brief method to add a physical stream
   * @caution combination of node and name has to be unique
   * @return bool indicating success of insert stream
   */
    bool addPhysicalStream(std::string logicalStreamName,
                           StreamCatalogEntryPtr entry);

    /**
   * @brief method to remove a physical stream
   * @caution this will not update the topology
   * @param logical stream where this physical stream reports to
   * @param structure describing the entry in the catalog
   * @return bool indicating success of remove stream
   */
    bool removePhysicalStream(string logicalStreamName, string physicalStreamName, std::size_t hashId);

    /**
   * @brief method to remove a physical stream from its logical streams
   * @param name of the logical stream
   * @param name of the physical stream
   * @param hashId of the actor
   * @return bool indicating success of remove stream
   */

    bool removePhysicalStreamByHashId(size_t hashId);

    /**
     * @brief method to remove a physical stream from its logical streams
     * @param hasId of the leaving node
     * @return bool indicating success of remove of physical stream
     */

    bool removeAllPhysicalStreams(std::string physicalStreamName);

    /**
   * @brief method to remove a physical stream from all logical streams
   * @param param of the node to be deleted
   * @return bool indicating success of remove stream
   */

    SchemaPtr getSchemaForLogicalStream(std::string logicalStreamName);

    /**
   * @brief method to return the stream for an existing logical stream
   * @param name of logical stream
   * @return smart pointer to a newly created stream
   * @note the stream will also contain the schema
   */
    StreamPtr getStreamForLogicalStream(std::string logicalStreamName);

    /**
   * @brief method to return the stream for an existing logical stream or throw exception
   * @param name of logical stream
   * @return smart pointer to a newly created stream
   * @note the stream will also contain the schema
   */
    StreamPtr getStreamForLogicalStreamOrThrowException(
        std::string logicalStreamName);

    /**
   * @brief test if logical stream with this name exists in the log to schema mapping
   * @param name of the logical stream to test
   * @return bool indicating if stream exists
   */
    bool testIfLogicalStreamExistsInSchemaMapping(std::string logicalStreamName);

    /**
   * @brief test if logical stream with this name exists in the log to phy mapping
   * @param name of the logical stream to test
   * @return bool indicating if stream exists
   */
    bool testIfLogicalStreamExistsInLogicalToPhysicalMapping(
        std::string logicalStreamName);

    /**
   * @brief return all physical nodes that contribute to this logical stream
   * @param name of logical stream
   * @return list of physical nodes as pointers into the topology
   */
    vector<NESTopologyEntryPtr> getSourceNodesForLogicalStream(
        std::string logicalStreamName);

    /**
   * @brief reset the catalog and recreate the default_logical stream
   * @return bool indicating success
   */
    bool reset();

    /**
   * @brief Return a list of logical stream names registered at catalog
   * @return map containing stream name as key and schema object as value
   */
    std::map<std::string, SchemaPtr> getAllLogicalStream();

    std::map<std::string, std::string> getAllLogicalStreamAsString();

    /**
       * @brief method to return the physical stream and the associated schemas
       * @return string containing the content of the catalog
       */
    std::string getPhysicalStreamAndSchemaAsString();

    /**
     * @brief get all pyhsical streams for a logical stream
     * @param logicalStreamName
     * @return
     */
    std::vector<StreamCatalogEntryPtr> getPhysicalStreams(
        std::string logicalStreamName);

    /**
     * @brief update an existing stream
     * @param streamName
     * @param streamSchema
     * @return
     */
    bool updatedLogicalStream(std::string& streamName, std::string& streamSchema);

    StreamCatalog();
    ~StreamCatalog();

  private:
    //map logical stream to schema
    std::map<std::string, SchemaPtr> logicalStreamToSchemaMapping;

    //map logical stream to physical source
    std::map<std::string, std::vector<StreamCatalogEntryPtr>> logicalToPhysicalStreamMapping;
};
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;
}// namespace NES
#endif /* INCLUDE_CATALOGS_STREAMCATALOG_HPP_ */
