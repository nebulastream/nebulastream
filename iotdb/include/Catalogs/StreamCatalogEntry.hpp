#ifndef INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_
#define INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_

#include <string>
#include <Topology/NESTopologyEntry.hpp>

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

}

#endif /* INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_ */
