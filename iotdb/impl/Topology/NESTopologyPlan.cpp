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
namespace NES {

std::vector<NESTopologyEntryPtr> NESTopologyPlan::getNodeByIp(std::string ip) {
  assert(0);
  return fGraphPtr->getVertexByIp(ip);
}

std::vector<NESTopologyEntryPtr> NESTopologyPlan::getNodeById(size_t id) {
  return fGraphPtr->getVertexById(id);
}

NESTopologyPlan::NESTopologyPlan() {
  fGraphPtr = std::make_shared<NESTopologyGraph>();
  currentLinkId = 0;
}

NESTopologyEntryPtr NESTopologyPlan::getRootNode() const {
  return fGraphPtr->getRoot();
}

NESTopologyCoordinatorNodePtr NESTopologyPlan::createNESCoordinatorNode(
    size_t id, std::string ipAddr, CPUCapacity cpuCapacity) {
  auto ptr = std::make_shared<NESTopologyCoordinatorNode>(id, ipAddr);
  ptr->setCpuCapacity(cpuCapacity);
  assert(fGraphPtr->addVertex(ptr));
  return ptr;
}

NESTopologyWorkerNodePtr NESTopologyPlan::createNESWorkerNode(
    size_t id, std::string ipAddr, CPUCapacity cpuCapacity) {
  // create worker node
  auto ptr = std::make_shared<NESTopologyWorkerNode>(id, ipAddr);
  ptr->setCpuCapacity(cpuCapacity);
  assert(fGraphPtr->addVertex(ptr));
  return ptr;
}

NESTopologySensorNodePtr NESTopologyPlan::createNESSensorNode(
    size_t id, std::string ip, CPUCapacity cpuCapacity) {
  // create sensor node
  auto ptr = std::make_shared<NESTopologySensorNode>(id, ip);
  ptr->setCpuCapacity(cpuCapacity);

  ptr->setPhysicalStreamName("default_physical");
  assert(fGraphPtr->addVertex(ptr));
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

NESTopologyLinkPtr NESTopologyPlan::createNESTopologyLink(
    NESTopologyEntryPtr pSourceNode, NESTopologyEntryPtr pDestNode,
    size_t pLinkCapacity, size_t pLinkLatency) {

  // check if link already exists
  if (fGraphPtr->hasLink(pSourceNode, pDestNode)) {
    // return already existing link
    NES_DEBUG("NESTopologyPlan::createNESTopologyLink: link between " << pSourceNode << " and " << pDestNode << " already exists")
    return fGraphPtr->getLink(pSourceNode, pDestNode);
  }

  // create new link
  size_t linkId = getNextFreeLinkId();
  NES_DEBUG("NESTopologyPlan::createNESTopologyLink: create a new link with id=" << linkId)
  auto linkPtr = std::make_shared<NESTopologyLink>(linkId, pSourceNode,
                                                   pDestNode, pLinkCapacity,
                                                   pLinkLatency);
  bool success = fGraphPtr->addEdge(linkPtr);
  if (!success) {
    NES_ERROR("NESTopologyPlan: could not add node");
    return nullptr;
  }
  return linkPtr;
}

bool NESTopologyPlan::removeNESTopologyLink(NESTopologyLinkPtr linkPtr) {
  return fGraphPtr->removeEdge(linkPtr->getId());
}

std::string NESTopologyPlan::getTopologyPlanString() const {
  return fGraphPtr->getGraphString();
}

size_t NESTopologyPlan::getNextFreeLinkId() {
  while (fGraphPtr->hasLink(currentLinkId)) {
    currentLinkId++;
  }
  return currentLinkId;
}

NESTopologyGraphPtr NESTopologyPlan::getNESTopologyGraph() const {
  return fGraphPtr;
}

}  // namespace NES
