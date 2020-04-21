#ifndef INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_
#define INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_

#include <string>
#include <Topology/NESTopologyEntry.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <sstream>

namespace NES {
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
  StreamCatalogEntry(PhysicalStreamConfig streamConf, NESTopologyEntryPtr node);

  std::string getSourceType();

  std::string getSourceConfig();

  NESTopologyEntryPtr getNode();

  std::string getPhysicalName();

  std::string getLogicalName();

  double getSourceFrequency();

  size_t getNumberOfBuffersToProduce();

  std::string toString();

 private:
  PhysicalStreamConfig streamConf;
  NESTopologyEntryPtr node;
};
typedef std::shared_ptr<StreamCatalogEntry> StreamCatalogEntryPtr;

}

#endif /* INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_ */
