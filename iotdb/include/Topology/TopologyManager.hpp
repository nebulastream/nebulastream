#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYMANAGER_HPP_

#include <memory>

#include <utility>

#include "NESTopologyPlan.hpp"
#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include "Util/CPUCapacity.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include <Topology/NESTopologyGraph.hpp>
#include <cpprest/json.h>

/**
 * TODO: add return of create
 */
namespace NES {

using namespace web;

//FIXME:add docu here
class TopologyManager {
  public:

    TopologyManager();

    /**
     * @brief create a nes coordinator node
     * @param id of the coordinator as size_t
     * @param ip of coordinator node as string
     * @param CPUCapacity of the node
     * @return NESTopologyCoordinatorNodePtr to the created coordinator
     */
    NESTopologyCoordinatorNodePtr createNESCoordinatorNode(size_t id,
                                                           const std::string ipAddr, CPUCapacity cpuCapacity);

    /**
     * @brief create a nes worker node
     * @param actor id of the node
     * @param ip of worker node as string
     * @param CPUCapacity of the node
     * @return NESTopologyWorkerNodePtr to the created worker
     */
    NESTopologyWorkerNodePtr createNESWorkerNode(size_t id, const std::string ipAddr,
                                                 CPUCapacity cpuCapacity);

    /**
     * @brief create a nes sensor node
     * @param id of the node
     * @param ip of sensor node as string
     * @param CPUCapacity of the node
     * @return NESTopologySensorNodePtr to the created sensor
     */
    NESTopologySensorNodePtr createNESSensorNode(size_t id, const std::string ipAddr,
                                                 CPUCapacity cpuCapacity);

    /**
     * @brief remove a nes worker node
     * @param NESTopologyWorkerNodePtr to the worker node to be deleted
     * @return bool indicating the success of the removal
     */
    bool removeNESWorkerNode(NESTopologyWorkerNodePtr ptr);

    /**
     * @brief remove a nes sensor node
     * @param NESTopologySensorNodePtr to the sensor node to be deleted
     * @return bool indicating the success of the removal
     */
    bool removeNESSensorNode(NESTopologySensorNodePtr ptr);

    /**
     * @brief remove a nes node
     * @param NESTopologyEntryPtr to the node to be deleted
     * @return bool indicating the success of the removal
     */
    bool removeNESNode(NESTopologyEntryPtr ptr);

    /**
     * @brief method to create a link between two nodes
     * @param NESTopologyEntryPtr to node 1 (source)
     * @param NESTopologyEntryPtr to node 2 (destination)
     * @param linkCapacity : link capacity
     * @param linkLatency : link latency
     * @return NESTopologyLinkPtr to the link, if failed return nullptr
     */
    NESTopologyLinkPtr createNESTopologyLink(NESTopologyEntryPtr pSourceNode,
                                             NESTopologyEntryPtr pDestNode,
                                             size_t pLinkCapacity,
                                             size_t pLinkLatency);

    /**
     * @brief method to remove a link
     * @param NESTopologyLinkPtr to the link to be deleted
     * @return bool indicating the success of the deletion
     */
    bool removeNESTopologyLink(NESTopologyLinkPtr linkPtr);

    /**
     * @brief print the topology to debug log
     */
    void printNESTopologyPlan();

    /**
     * @brief get the topology as string
     * @return string containing the topology
     */
    std::string getNESTopologyPlanString();

    /**
     * @brief method to get the topology plan
     * @return NESTopologyPlanPtr to the topology
     */
    NESTopologyPlanPtr getNESTopologyPlan();

    /**
     * @brief get root not of topology
     * @return NESTopologyEntryPtr to the root node
     */
    NESTopologyEntryPtr getRootNode();

    /**
     * @brief method to get the topology as json
     * @return topology as json
     */
    json::value getNESTopologyGraphAsJson();

    /**
     * @brief get children of nodes as a vector of json objects
     * @return vector of json objects of children
     */
    std::vector<json::value> getChildrenNodes(NESTopologyEntryPtr nesParentNode);

    /**
     * @brief method to reset the topology
     */
    void resetNESTopologyPlan();

    web::json::value getNESTopologyAsJson();

  private:
    NESTopologyPlanPtr currentPlan;
};
typedef std::shared_ptr<TopologyManager> TopologyManagerPtr;

}// namespace NES
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYMANAGER_HPP_ */
