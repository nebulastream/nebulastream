#ifndef INCLUDE_CATALOGS_STREAMCATALOG_HPP_
#define INCLUDE_CATALOGS_STREAMCATALOG_HPP_

#include <string>
#include <vector>
#include <map>
#include <deque>
#include <SourceSink/DataSource.hpp>
#include <API/Schema.hpp>

#include <Topology/NESTopologyEntry.hpp>

using namespace std;
namespace iotdb {

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
  StreamCatalogEntry(std::string dataSourceType, std::string dataSourceConfig,
                     NESTopologyEntryPtr node, std::string physicalStreamName)
      : dataSourceType(dataSourceType),
        dataSourceConfig(dataSourceConfig),
        node(node),
        physicalStreamName(physicalStreamName) {
  }
  ;

  std::string getSourceType() {
    return dataSourceType;
  }

  std::string getSourceConfig() {
    return dataSourceConfig;
  }

  NESTopologyEntryPtr getNode() {
    return node;
  }

  std::string getPhysicalName() {
    return physicalStreamName;
  }

 private:
  std::string dataSourceType;
  std::string dataSourceConfig;
  NESTopologyEntryPtr node;
  std::string physicalStreamName;
};
typedef std::shared_ptr<StreamCatalogEntry> StreamCatalogEntryPtr;

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
   * @caution there is only one schema per logical stream allowed
   */
  SchemaPtr getSchemaForLogicalStream(std::string logicalStreamName);

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

  void reset();
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
