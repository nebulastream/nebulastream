#ifndef INCLUDE_CATALOGS_STREAMCATALOG_HPP_
#define INCLUDE_CATALOGS_STREAMCATALOG_HPP_

#include <string>
#include <vector>
#include <map>
#include <deque>
#include <SourceSink/DataSource.hpp>
#include <API/Schema.hpp>
#include <API/Stream.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
using namespace std;
namespace iotdb {

/**
 * @brief the stream catalog handles the mapping of logical to physical streams
 * @Limitations
 *    - TODO: do we really want to identify streams by name or would it be better to introduce an id?
 *    - TODO: add mutex to make it secure
 */
class StreamCatalog {
 public:
  /**
   * @brief Singleton implementation of stream catalog
   */
  static StreamCatalog& instance();

  /**
   * @brief method to add a logical stream
   * @param logical stream name
   * @param schema of logical stream
   */
  void addLogicalStream(std::string logicalStreamName, SchemaPtr schemaPtr);

  /**
   * @brief method to add a physical stream
   * @caution combination of node and name has to be unique
   */
  void addPhysicalStream(std::string logicalStreamName,
                         StreamCatalogEntryPtr entry);

  /**
   * @brief method to return the schema for an existing logical stream
   * @param name of logical stream
   * @return smart pointer to the schema
   * @caution there is only one schema per logical stream allowed
   */
  SchemaPtr getSchemaForLogicalStream(std::string logicalStreamName);


  /**
   * @brief method to return the schema for an existing logical stream or throw exception
   * @param name of logical stream
   * @return smart pointer to the schema
   * @caution there is only one schema per logical stream allowed
   */
  SchemaPtr getSchemaForLogicalStreamOrThrowException(std::string logicalStreamName);


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
   StreamPtr getStreamForLogicalStreamOrThrowException(std::string logicalStreamName);


  /**
   * @brief test if logical stream with this name exists
   * @param name of the logical stream to test
   * @return bool indicating if stream exists
   */
  bool testIfLogicalStreamExists(std::string logicalStreamName);

  /**
   * @brief return all physical nodes that contribute to this logical stream
   * @param name of logical stream
   * @return list of physical nodes as pointers into the topology
   */
  deque<NESTopologyEntryPtr> getSourceNodesForLogicalStream(
      std::string logicalStreamName);

  /**
   * @brief reset the catalog and recreate the default_logical stream
   */
  void reset();

  /**
   * @brief method to return the logical stream and the associated schemas
   * @return string containing the content of the catalog
   */
  std::string getLogicalStreamAndSchemaAsString();

  /**
   * @brief method to return the physical stream and the associated schemas
   * @return string containing the content of the catalog
   */
  std::string getPhysicalStreamAndSchemaAsString();


 private:
  /* implement singleton semantics: no construction,
   * copying or destruction of stream catalog objects
   * outside of the class
   * Default thread count is 1
   */
  StreamCatalog();
  ~StreamCatalog();

  //map logical stream to schema
  std::map<std::string, SchemaPtr> logicalStreamToSchemaMapping;

  //map logical stream to physical source
  std::map<std::string, std::vector<StreamCatalogEntryPtr>> logicalToPhysicalStreamMapping;

};
}
#endif /* INCLUDE_CATALOGS_STREAMCATALOG_HPP_ */
