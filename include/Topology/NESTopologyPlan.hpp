#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYPLAN_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYPLAN_HPP_

#include <memory>
#include <string>

#include "NESTopologyCoordinatorNode.hpp"
#include "NESTopologyEntry.hpp"
#include "NESTopologyLink.hpp"
#include "NESTopologySensorNode.hpp"
#include "NESTopologyWorkerNode.hpp"
#include "Util/CPUCapacity.hpp"
#include <Topology/NESTopologyGraph.hpp>

namespace NES {

/**
 * @brief This class represents the NESTopology Plan
 */
class NESTopologyPlan {

  public:
    NESTopologyPlan();

    /**
     * @brief method to get the root of the topology
     * @return NESTopologyEntryPtr to the root node
     */
    NESTopologyEntryPtr getRootNode() const;

    /**
     * @brief method to create the coordinator node
     * @param id: id of the node
     * @param ip: ip address of the node
     * @param CPUCapacity of the node
     * @return NESTopologyCoordinatorNodePtr of the created node
     */
    NESTopologyCoordinatorNodePtr createNESCoordinatorNode(size_t id, std::string ip, CPUCapacity cpuCapacity);

    /**
     * @brief method to create a worker node
     * @param id: id of the node
     * @param ip: ip address of the node
     * @param grpcPort: grpc port number
     * @param dataPort: data port number
     * @param CPUCapacity of the node
     * @return NESTopologyWorkerNodePtr of the created node
     */
    NESTopologyWorkerNodePtr createNESWorkerNode(size_t id, std::string ip, int32_t grpcPort, int32_t dataPort, CPUCapacity cpuCapacity);

    /**
     * @brief method to remove a worker node
     * @param NESTopologyWorkerNodePtr to the node to be removed
     * @return bool indicating the success of the removal
     */
    bool removeNESWorkerNode(NESTopologyWorkerNodePtr ptr);

    /**
     * @brief method to create a sensor node
     * @param id: id of the node
     * @param ip: ip address of the node
     * @param grpcPort: grpc port number
     * @param dataPort: data port number
     * @param CPUCapacity of the node
     * @return NESTopologySensorNodePtr of the created node
     */
    NESTopologySensorNodePtr createNESSensorNode(size_t id, std::string ip, int32_t grpcPort, int32_t dataPort, CPUCapacity cpuCapacity);

    /**
     * @brief method to remove a sensor node
     * @param NESTopologySensorNodePtr to the node to be removed
     * @return bool indicating the success of the removal
     */
    bool removeNESSensorNode(NESTopologySensorNodePtr ptr);

    /**
     * @brief method to create a link between two nodes
     * @param sourceNode : NESTopologyEntryPtr to first node (source)
     * @param destNode : NESTopologyEntryPtr to second node (destination)
     * @param linkCapacity : link capacity
     * @param linkLatency : link latency
     * @return NESTopologyLinkPtr of the created link, a nullptr if it could not be created
     */
    NESTopologyLinkPtr createNESTopologyLink(NESTopologyEntryPtr sourceNode,
                                             NESTopologyEntryPtr destNode,
                                             size_t linkCapacity,
                                             size_t linkLatency);

    /**
     * @brief method to remove a topology link
     * @param NESTopologyLinkPtr to the link to be removed
     * @return bool indicating the success of the deletion
     */
    bool removeNESTopologyLink(NESTopologyLinkPtr linkPtr);

    /**
     * @brief method to remove a nes node
     * @param NESTopologyEntryPtr to the node
     * @return bool indication the success of the removal
     */
    bool removeNESNode(NESTopologyEntryPtr ptr);

    /**
     * @brief method to the get topology as a string
     * @return string containing the topology
     */
    std::string getTopologyPlanString() const;

    /**
     * @brief method to the get NES graph
     * @return NESTopologyGraphPtr to the graph
     */
    NESTopologyGraphPtr getNESTopologyGraph() const;

    /**
     * @brief get all nodes with an ip
     * @param ip of the node
     * @return vector containing NESTopologyEntryPtr to the nodes
     */
    std::vector<NESTopologyEntryPtr> getNodeByIp(std::string ip);

    /**
     * @brief get all nodes with an id
     * @param id of the node
     * @return vector containing NESTopologyEntryPtr to the nodes
     */
    std::vector<NESTopologyEntryPtr> getNodeById(size_t id);

    size_t getNextFreeLinkId();
    size_t currentLinkId;
    NESTopologyGraphPtr fGraphPtr;
};
typedef std::shared_ptr<NESTopologyPlan> NESTopologyPlanPtr;
}// namespace NES

#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYPLAN_HPP_ */
