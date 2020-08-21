#ifndef INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_
#define INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <memory>
#include <sstream>
#include <string>

namespace NES {

class PhysicalNode;
typedef std::shared_ptr<PhysicalNode> PhysicalNodePtr;

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
    StreamCatalogEntry(PhysicalStreamConfig streamConf, PhysicalNodePtr node);

    std::string getSourceType();

    std::string getSourceConfig();

    PhysicalNodePtr getNode();

    std::string getPhysicalName();

    std::string getLogicalName();

    double getSourceFrequency();

    size_t getNumberOfBuffersToProduce();

    std::string toString();

  private:
    PhysicalStreamConfig streamConf;
    PhysicalNodePtr node;
};
typedef std::shared_ptr<StreamCatalogEntry> StreamCatalogEntryPtr;

}// namespace NES

#endif /* INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_ */
