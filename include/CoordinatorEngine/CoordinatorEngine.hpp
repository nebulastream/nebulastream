#ifndef NES_INCLUDE_COORDINATORENGINE_COORDINATORENGINE_HPP_
#define NES_INCLUDE_COORDINATORENGINE_COORDINATORENGINE_HPP_
#include <NodeStats.pb.h>
#include <Topology/NESTopologyEntry.hpp>
#include <memory>
#include <mutex>

namespace NES {
class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class TopologyManager;
typedef std::shared_ptr<TopologyManager> TopologyManagerPtr;

class CoordinatorEngine {

  public:
    CoordinatorEngine(StreamCatalogPtr streamCatalog, TopologyManagerPtr topologyManager);

    /**
     * @brief registers a node
     * @param address of node ip:port
     * @param cpu the cpu capacity of the worker
     * @param nodeProperties of the to be added sensor
     * @param node type
     * @return id of node
     */
    size_t registerNode(std::string address, int64_t grpcPort, int64_t dataPort, int8_t numberOfCPUs, NodeStats nodeStats, NESNodeType type);

    /**
     * @brief unregister an existing node
     * @param nodeId
     * @return bool indicating success
     */
    bool unregisterNode(size_t nodeId);

    /**
     * @brief method to register a physical stream
     * @param nodeId
     * @param sourcetype
     * @param sourceconf
     * @param sourcefrequency
     * @param numberofbufferstoproduce
     * @param physicalstreamname
     * @param logicalstreamname
     * @return bool indicating success
     */
    bool registerPhysicalStream(size_t nodeId,
                                std::string sourcetype,
                                std::string sourceconf,
                                size_t sourcefrequency,
                                size_t numberofbufferstoproduce,
                                std::string physicalstreamname,
                                std::string logicalstreamname);

    /**
     * @brief method to unregister a physical stream
     * @param nodeId
     * @param logicalStreamName
     * @param physicalStreamName
     * @return bool indicating success
     */
    bool unregisterPhysicalStream(size_t nodeId, std::string physicalStreamName, std::string logicalStreamName);

    /**
     * @brief method to register a logical stream
     * @param logicalStreamName
     * @param schemaString
     * @return bool indicating success
     */
    bool registerLogicalStream(std::string logicalStreamName, std::string schemaString);

    /**
     * @brief method to unregister a logical stream
     * @param logicalStreamName
     * @return bool indicating success
     */
    bool unregisterLogicalStream(std::string logicalStreamName);

    /**
     * @brief method to ad a new parent to a node
     * @param childId
     * @param parentId
     * @return bool indicating success
     */
    bool addParent(size_t childId, size_t parentId);

    /**
     * @brief method to remove an existing parent from a node
     * @param childId
     * @param parentId
     * @return bool indicating success
     */
    bool removeParent(size_t childId, size_t parentId);

  private:
    StreamCatalogPtr streamCatalog;
    TopologyManagerPtr topologyManager;
    std::mutex registerDeregisterNode;
    std::mutex addRemoveLogicalStream;
    std::mutex addRemovePhysicalStream;
};

typedef std::shared_ptr<CoordinatorEngine> CoordinatorEnginePtr;

}// namespace NES

#endif//NES_INCLUDE_COORDINATORENGINE_COORDINATORENGINE_HPP_
