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
    StreamCatalogEntry(PhysicalStreamConfig streamConf, TopologyNodePtr node);

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
    PhysicalStreamConfig streamConf;
    TopologyNodePtr node;
};
typedef std::shared_ptr<StreamCatalogEntry> StreamCatalogEntryPtr;

}// namespace NES

#endif /* INCLUDE_CATALOGS_STREAMCATALOGENTRY_HPP_ */
