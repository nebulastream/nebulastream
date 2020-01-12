#include <cassert>
#include <memory>
#include <sstream>
#include <string>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graph_traits.hpp>
#include <boost/graph/graphviz.hpp>

#include <Topology/NESTopologyPlan.hpp>
#include <Topology/NESTopologyCoordinatorNode.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <Topology/NESTopologyGraph.hpp>
#include <Topology/NESTopologyLink.hpp>
#include <Topology/NESTopologySensorNode.hpp>
#include <Topology/NESTopologyWorkerNode.hpp>
#include <Util/CPUCapacity.hpp>
#include <Util/Logger.hpp>
namespace iotdb {

std::vector<NESTopologyEntryPtr> NESTopologyPlan::getNodeByIp(std::string ip)
{
//  IOTDB_NOT_IMPLEMENTED
   return fGraphPtr->getVertexByIp(ip);
}

NESTopologyPlan::NESTopologyPlan() {
  fGraphPtr = std::make_shared<NESTopologyGraph>();
  currentNodeId = 0;
  currentLinkId = 0;
}

NESTopologyEntryPtr NESTopologyPlan::getRootNode() const { return fGraphPtr->getRoot(); }

NESTopologyCoordinatorNodePtr NESTopologyPlan::createNESCoordinatorNode(std::string ipAddr, CPUCapacity cpuCapacity) {
  // create coordinator node
  size_t nodeId = getNextFreeNodeId();
  auto ptr = std::make_shared<NESTopologyCoordinatorNode>(nodeId, ipAddr);
  ptr->setCpuCapacity(cpuCapacity);
  fGraphPtr->addVertex(ptr);
  return ptr;
}

NESTopologyWorkerNodePtr NESTopologyPlan::createNESWorkerNode(std::string ipAddr, CPUCapacity cpuCapacity) {
  // create worker node
  size_t nodeId = getNextFreeNodeId();
  auto ptr = std::make_shared<NESTopologyWorkerNode>(nodeId, ipAddr);
  ptr->setCpuCapacity(cpuCapacity);
  fGraphPtr->addVertex(ptr);
  return ptr;
}

NESTopologySensorNodePtr NESTopologyPlan::createNESSensorNode(std::string ipAddr, CPUCapacity cpuCapacity) {
  // create sensor node
  size_t nodeId = getNextFreeNodeId();
  auto ptr = std::make_shared<NESTopologySensorNode>(nodeId, ipAddr);
  ptr->setCpuCapacity(cpuCapacity);
  ptr->setPhysicalStreamName("default_physical");
  fGraphPtr->addVertex(ptr);
  return ptr;
}

bool NESTopologyPlan::removeNESWorkerNode(NESTopologyWorkerNodePtr ptr) {
  return fGraphPtr->removeVertex(ptr->getId());
}

bool NESTopologyPlan::removeNESSensorNode(NESTopologySensorNodePtr ptr) {
  return fGraphPtr->removeVertex(ptr->getId());
}

bool NESTopologyPlan::removeNESNode(NESTopologyEntryPtr ptr) {
  return fGraphPtr->removeVertex(ptr->getId());
}

NESTopologyLinkPtr NESTopologyPlan::createNESTopologyLink(NESTopologyEntryPtr pSourceNode,
                                                          NESTopologyEntryPtr pDestNode) {

  // check if link already exists
  if (fGraphPtr->hasLink(pSourceNode, pDestNode)) {
    // return already existing link
    return fGraphPtr->getLink(pSourceNode, pDestNode);
  }

  // create new link
  size_t linkId = getNextFreeLinkId();
  auto linkPtr = std::make_shared<NESTopologyLink>(linkId, pSourceNode, pDestNode);
  bool success = fGraphPtr->addEdge(linkPtr);
  if(!success)
  {
    IOTDB_ERROR("NESTopologyPlan: could not add node");
    return nullptr;
  }
  return linkPtr;
}

bool NESTopologyPlan::removeNESTopologyLink(NESTopologyLinkPtr linkPtr) {
  return fGraphPtr->removeEdge(linkPtr->getId());
}

std::string NESTopologyPlan::getTopologyPlanString() const { return fGraphPtr->getGraphString(); }

size_t NESTopologyPlan::getNextFreeNodeId() {
  while (fGraphPtr->hasVertex(currentNodeId)) {
    currentNodeId++;
  }
  return currentNodeId;
}

size_t NESTopologyPlan::getNextFreeLinkId() {
  while (fGraphPtr->hasLink(currentLinkId)) {
    currentLinkId++;
  }
  return currentLinkId;
}

NESGraphPtr NESTopologyPlan::getNESGraph() const {
  return fGraphPtr;
}

} // namespace iotdb
