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

class NESTopologyManager {
 public:
  static NESTopologyManager &getInstance() {
    static NESTopologyManager instance; // Guaranteed to be destroyed.
    // Instantiated on first use.
    return instance;
  }

  NESTopologyManager(NESTopologyManager const &); // Don't Implement
  void operator=(NESTopologyManager const &);     // Don't implement

  FogTopologyCoordinatorNodePtr createFogCoordinatorNode(const std::string ipAddr, CPUCapacity cpuCapacity) {
    return currentPlan->createFogCoordinatorNode(ipAddr, cpuCapacity);
  }

  NESTopologyWorkerNodePtr createFogWorkerNode(const std::string ipAddr, CPUCapacity cpuCapacity) {
    return currentPlan->createFogWorkerNode(ipAddr, cpuCapacity);
  }

  FogTopologySensorNodePtr createFogSensorNode(const std::string ipAddr, CPUCapacity cpuCapacity) {
    return currentPlan->createFogSensorNode(ipAddr, cpuCapacity);
  }

  bool removeFogWorkerNode(NESTopologyWorkerNodePtr ptr) { return currentPlan->removeFogWorkerNode(std::move(ptr)); }

  bool removeFogSensorNode(FogTopologySensorNodePtr ptr) { return currentPlan->removeFogSensorNode(std::move(ptr)); }

  bool removeFogNode(NESTopologyEntryPtr ptr) { return currentPlan->removeFogNode(std::move(ptr)); }

  NESTopologyLinkPtr createFogTopologyLink(NESTopologyEntryPtr pSourceNode, NESTopologyEntryPtr pDestNode) {
    return currentPlan->createFogTopologyLink(pSourceNode, pDestNode);
  }

  bool removeFogTopologyLink(NESTopologyLinkPtr linkPtr) { return currentPlan->removeFogTopologyLink(linkPtr); }

  void printTopologyPlan() { std::cout << getTopologyPlanString() << std::endl; }

  std::string getTopologyPlanString() { return currentPlan->getTopologyPlanString(); }

  NESTopologyPlanPtr getTopologyPlan() { return currentPlan; }

  NESTopologyEntryPtr getRootNode() { return currentPlan->getRootNode(); };

  /**\brief:
   *          This is a temporary method used for simulating an example topology.
   *
   */
  void createExampleTopology() {

    resetFogTopologyPlan();

    const FogTopologyCoordinatorNodePtr &sinkNode = createFogCoordinatorNode("localhost", CPUCapacity::HIGH);
    const NESTopologyWorkerNodePtr &workerNode1 = createFogWorkerNode("localhost", CPUCapacity::MEDIUM);
    const NESTopologyWorkerNodePtr &workerNode2 = createFogWorkerNode("localhost", CPUCapacity::MEDIUM);
    const FogTopologySensorNodePtr &sensorNode1 = createFogSensorNode("localhost", CPUCapacity::HIGH);
    sensorNode1->setSensorType("temperature1");
    const FogTopologySensorNodePtr &sensorNode2 = createFogSensorNode("localhost", CPUCapacity::LOW);
    sensorNode2->setSensorType("humidity1");
    const FogTopologySensorNodePtr &sensorNode3 = createFogSensorNode("localhost", CPUCapacity::LOW);
    sensorNode3->setSensorType("temperature2");
    const FogTopologySensorNodePtr &sensorNode4 = createFogSensorNode("localhost", CPUCapacity::MEDIUM);
    sensorNode4->setSensorType("humidity2");

    createFogTopologyLink(workerNode1, sinkNode);
    createFogTopologyLink(workerNode2, sinkNode);
    createFogTopologyLink(sensorNode1, workerNode1);
    createFogTopologyLink(sensorNode2, workerNode1);
    createFogTopologyLink(sensorNode3, workerNode2);
    createFogTopologyLink(sensorNode4, workerNode2);
  }

  json::value getFogTopologyGraphAsJson() {

    const NESGraphPtr &fogGraphPtr = getTopologyPlan()->getFogGraph();
    const std::vector<NESTopologyLinkPtr> &allEdges = fogGraphPtr->getAllEdges();
    const std::vector<NESVertex> &allVertex = fogGraphPtr->getAllVertex();

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
        FogTopologySensorNodePtr ptr = std::static_pointer_cast<NESTopologySensorNode>(vertex.ptr);
        vertexInfo["sensorType"] = json::value::string(ptr->getSensorType());
      }

      vertices.push_back(vertexInfo);
    }

    result["nodes"] = json::value::array(vertices);
    result["edges"] = json::value::array(edges);
    return result;
  }

  std::vector<json::value> getChildrenNode(NESTopologyEntryPtr fogParentNode) {

    const NESGraphPtr &fogGraphPtr = getTopologyPlan()->getFogGraph();
    const std::vector<NESTopologyLinkPtr> &edgesToNode = fogGraphPtr->getAllEdgesToNode(fogParentNode);

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

  void resetFogTopologyPlan() {
    currentPlan.reset(new NESTopologyPlan());
  }

 private:
  NESTopologyManager() { currentPlan = std::make_shared<NESTopologyPlan>(); }

  NESTopologyPlanPtr currentPlan;
};
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYMANAGER_HPP_ */
