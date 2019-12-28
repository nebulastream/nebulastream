#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYMANAGER_HPP_

#include <memory>

#include <utility>

#include "NESTopologyPlan.hpp"
#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include <cpprest/json.h>
#include "Util/CPUCapacity.hpp"

/**
 * TODO: add return of create
 */
namespace iotdb {

using namespace web;

//FIXME:add docu here
class NESTopologyManager {
 public:
  static NESTopologyManager &getInstance() {
    static NESTopologyManager instance; // Guaranteed to be destroyed.
    // Instantiated on first use.
    return instance;
  }

  NESTopologyManager(NESTopologyManager const &); // Don't Implement
  void operator=(NESTopologyManager const &);     // Don't implement

  NESTopologyCoordinatorNodePtr createNESCoordinatorNode(const std::string ipAddr, CPUCapacity cpuCapacity) {
    return currentPlan->createNESCoordinatorNode(ipAddr, cpuCapacity);
  }

  NESTopologyWorkerNodePtr createNESWorkerNode(const std::string ipAddr, CPUCapacity cpuCapacity) {
    return currentPlan->createNESWorkerNode(ipAddr, cpuCapacity);
  }

  NESTopologySensorNodePtr createNESSensorNode(const std::string ipAddr, CPUCapacity cpuCapacity) {
    return currentPlan->createNESSensorNode(ipAddr, cpuCapacity);
  }

  bool removeNESWorkerNode(NESTopologyWorkerNodePtr ptr) { return currentPlan->removeNESWorkerNode(std::move(ptr)); }

  bool removeNESSensorNode(NESTopologySensorNodePtr ptr) { return currentPlan->removeNESSensorNode(std::move(ptr)); }

  bool removeNESNode(NESTopologyEntryPtr ptr) { return currentPlan->removeNESNode(std::move(ptr)); }

  NESTopologyLinkPtr createNESTopologyLink(NESTopologyEntryPtr pSourceNode, NESTopologyEntryPtr pDestNode) {
    return currentPlan->createNESTopologyLink(pSourceNode, pDestNode);
  }

  bool removeNESTopologyLink(NESTopologyLinkPtr linkPtr) { return currentPlan->removeNESTopologyLink(linkPtr); }

  void printNESTopologyPlan() { std::cout << getNESTopologyPlanString() << std::endl; }

  std::string getNESTopologyPlanString() { return currentPlan->getTopologyPlanString(); }

  NESTopologyPlanPtr getNESTopologyPlan() { return currentPlan; }

  NESTopologyEntryPtr getRootNode() { return currentPlan->getRootNode(); };

  /**\brief:
   *          This is a temporary method used for simulating an example topology.
   *
   */
  void createExampleTopology() {

    resetNESTopologyPlan();

    const NESTopologyCoordinatorNodePtr &sinkNode = createNESCoordinatorNode("localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr &workerNode1 = createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode2 = createNESWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologySensorNodePtr &sensorNode1 = createNESSensorNode("localhost", CPUCapacity::HIGH);
    sensorNode1->setPhysicalStreamName("temperature1");
    const NESTopologySensorNodePtr &sensorNode2 = createNESSensorNode("localhost", CPUCapacity::LOW);
    sensorNode2->setPhysicalStreamName("humidity1");
    const NESTopologySensorNodePtr &sensorNode3 = createNESSensorNode("localhost", CPUCapacity::LOW);
    sensorNode3->setPhysicalStreamName("temperature2");
    const NESTopologySensorNodePtr &sensorNode4 = createNESSensorNode("localhost", CPUCapacity::MEDIUM);
    sensorNode4->setPhysicalStreamName("humidity2");

    createNESTopologyLink(workerNode1, sinkNode);
    createNESTopologyLink(workerNode2, sinkNode);
    createNESTopologyLink(sensorNode1, workerNode1);
    createNESTopologyLink(sensorNode2, workerNode1);
    createNESTopologyLink(sensorNode3, workerNode2);
    createNESTopologyLink(sensorNode4, workerNode2);
  }

  json::value getNESTopologyGraphAsJson() {

    const NESGraphPtr &nesGraphPtr = getNESTopologyPlan()->getNESGraph();
    const std::vector<NESTopologyLinkPtr> &allEdges = nesGraphPtr->getAllEdges();
    const std::vector<NESVertex> &allVertex = nesGraphPtr->getAllVertex();

    auto result = json::value::object();
    std::vector<json::value> edges{};
    std::vector<json::value> vertices{};
    for (u_int i = 0; i < allEdges.size(); i++) {
      const NESTopologyLinkPtr &edge = allEdges[i];
      const NESTopologyEntryPtr &sourceNode = edge->getSourceNode();
      const NESTopologyEntryPtr &destNode = edge->getDestNode();
      auto edgeInfo = json::value::object();
      const auto source = "Node-" + std::to_string(sourceNode->getId());
      const auto dest = "Node-" + std::to_string(destNode->getId());
      edgeInfo["source"] = json::value::string(source);
      edgeInfo["target"] = json::value::string(dest);
      edges.push_back(edgeInfo);
    }

    for (u_int i = 0; i < allVertex.size(); i++) {
      const NESVertex &vertex = allVertex[i];
      auto vertexInfo = json::value::object();
      const std::string id = "Node-" + std::to_string(vertex.ptr->getId());
      const std::string nodeType = vertex.ptr->getEntryTypeString();
      int cpuCapacity = vertex.ptr->getCpuCapacity();
      int remainingCapacity = vertex.ptr->getRemainingCpuCapacity();

      vertexInfo["id"] = json::value::string(id);
      vertexInfo["title"] = json::value::string(id);
      vertexInfo["nodeType"] = json::value::string(nodeType);
      vertexInfo["capacity"] = json::value::string(std::to_string(cpuCapacity));
      vertexInfo["remainingCapacity"] = json::value::string(std::to_string(remainingCapacity));

      if (nodeType == "Sensor") {
        NESTopologySensorNodePtr ptr = std::static_pointer_cast<NESTopologySensorNode>(vertex.ptr);
        //TODO: ankit please check if this ok for you
        vertexInfo["physicalStreamName"] = json::value::string(ptr->getPhysicalStreamName());
      }

      vertices.push_back(vertexInfo);
    }

    result["nodes"] = json::value::array(vertices);
    result["edges"] = json::value::array(edges);
    return result;
  }

  std::vector<json::value> getChildrenNode(NESTopologyEntryPtr nesParentNode) {

    const NESGraphPtr &nesGraphPtr = getNESTopologyPlan()->getNESGraph();
    const std::vector<NESTopologyLinkPtr> &edgesToNode = nesGraphPtr->getAllEdgesToNode(nesParentNode);

    std::vector<json::value> children = {};

    if (edgesToNode.empty()) {
      return {};
    }

    for (NESTopologyLinkPtr edge: edgesToNode) {
      const NESTopologyEntryPtr &sourceNode = edge->getSourceNode();
      if (sourceNode) {
        auto child = json::value::object();
        const auto label = std::to_string(sourceNode->getId()) + "-" + sourceNode->getEntryTypeString();
        child["id"] = json::value::string(label);
        const std::vector<json::value> &grandChildren = getChildrenNode(sourceNode);
        if (!grandChildren.empty()) {
          child["children"] = json::value::array(grandChildren);
        }
        children.push_back(child);
      }
    }
    return children;
  }

  void resetNESTopologyPlan() {
    currentPlan.reset(new NESTopologyPlan());
  }

 private:
  NESTopologyManager() { currentPlan = std::make_shared<NESTopologyPlan>(); }

  NESTopologyPlanPtr currentPlan;
};
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYMANAGER_HPP_ */
