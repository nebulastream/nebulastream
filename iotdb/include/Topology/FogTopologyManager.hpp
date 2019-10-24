#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_

#include <memory>

#include <Topology/FogTopologyPlan.hpp>
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

typedef std::shared_ptr<FogTopologyPlan> FogTopologyPlanPtr;

class FogTopologyManager {
 public:
  static FogTopologyManager &getInstance() {
    static FogTopologyManager instance; // Guaranteed to be destroyed.
    // Instantiated on first use.
    return instance;
  }

  FogTopologyManager(FogTopologyManager const &); // Don't Implement
  void operator=(FogTopologyManager const &);     // Don't implement

  FogTopologyWorkerNodePtr createFogWorkerNode(CPUCapacity cpuCapacity) {
    return currentPlan->createFogWorkerNode(cpuCapacity);
  }

  bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr) { return currentPlan->removeFogWorkerNode(ptr); }

  bool removeFogSensorNode(FogTopologySensorNodePtr ptr) { return currentPlan->removeFogSensorNode(ptr); }

  FogTopologySensorNodePtr createFogSensorNode(CPUCapacity cpuCapacity) {
    return currentPlan->createFogSensorNode(cpuCapacity);
  }

  FogTopologyLinkPtr createFogTopologyLink(FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode) {
    return currentPlan->createFogTopologyLink(pSourceNode, pDestNode);
  }

  bool removeFogTopologyLink(FogTopologyLinkPtr linkPtr) { return currentPlan->removeFogTopologyLink(linkPtr); }

  void printTopologyPlan() { std::cout << getTopologyPlanString() << std::endl; }

  std::string getTopologyPlanString() { return currentPlan->getTopologyPlanString(); }

  FogTopologyPlanPtr getTopologyPlan() { return currentPlan; }

  FogTopologyEntryPtr getRootNode() { return currentPlan->getRootNode(); };

  /**\brief:
   *          This is a temporary method used for simulating an example topology.
   *
   */
  void createExampleTopology() {

    resetFogTopologyPlan();

    const FogTopologyWorkerNodePtr &sinkNode = createFogWorkerNode(CPUCapacity::HIGH);
    const FogTopologyWorkerNodePtr &workerNode1 = createFogWorkerNode(CPUCapacity::MEDIUM);
    const FogTopologyWorkerNodePtr &workerNode2 = createFogWorkerNode(CPUCapacity::MEDIUM);
    const FogTopologySensorNodePtr &sensorNode1 = createFogSensorNode(CPUCapacity::HIGH);
    sensorNode1->setSensorType("temperature1");
    const FogTopologySensorNodePtr &sensorNode2 = createFogSensorNode(CPUCapacity::LOW);
    sensorNode2->setSensorType("humidity1");
    const FogTopologySensorNodePtr &sensorNode3 = createFogSensorNode(CPUCapacity::LOW);
    sensorNode3->setSensorType("temperature2");
    const FogTopologySensorNodePtr &sensorNode4 = createFogSensorNode(CPUCapacity::MEDIUM);
    sensorNode4->setSensorType("humidity2");

    createFogTopologyLink(workerNode1, sinkNode);
    createFogTopologyLink(workerNode2, sinkNode);
    createFogTopologyLink(sensorNode1, workerNode1);
    createFogTopologyLink(sensorNode2, workerNode1);
    createFogTopologyLink(sensorNode3, workerNode2);
    createFogTopologyLink(sensorNode4, workerNode2);
  }

  json::value getFogTopologyGraphAsJson() {

    const FogGraph &graph = getTopologyPlan()->getFogGraph();
    const std::vector<FogEdge> &allEdges = graph.getAllEdges();
    const std::vector<FogVertex> &allVertex = graph.getAllVertex();

    auto result = json::value::object();
    std::vector<json::value> edges{};
    std::vector<json::value> vertices{};
    for (u_int i = 0; i < allEdges.size(); i++) {
      const FogEdge &edge = allEdges[i];
      const FogTopologyEntryPtr &sourceNode = edge.ptr->getSourceNode();
      const FogTopologyEntryPtr &destNode = edge.ptr->getDestNode();
      auto edgeInfo = json::value::object();
      const auto source = "Node-" + std::to_string(sourceNode->getId());
      const auto dest = "Node-" + std::to_string(destNode->getId());
      edgeInfo["source"] = json::value::string(source);
      edgeInfo["target"] = json::value::string(dest);
      edges.push_back(edgeInfo);
    }

    for (u_int i = 0; i < allVertex.size(); i++) {
      const FogVertex &vertex = allVertex[i];
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
        FogTopologySensorNodePtr ptr = std::static_pointer_cast<FogTopologySensorNode>(vertex.ptr);
        vertexInfo["sensorType"] = json::value::string(ptr->getSensorType());
      }

      vertices.push_back(vertexInfo);
    }

    result["nodes"] = json::value::array(vertices);
    result["edges"] = json::value::array(edges);
    return result;
  }

  std::vector<json::value> getChildrenNode(FogTopologyEntryPtr fogParentNode) {

    const FogGraph &fogGraph = getTopologyPlan()->getFogGraph();
    const std::vector<FogEdge> &edgesToNode = fogGraph.getAllEdgesToNode(fogParentNode);

    std::vector<json::value> children = {};

    if (edgesToNode.empty()) {
      return {};
    }

    for (FogEdge edge: edgesToNode) {
      const FogTopologyEntryPtr &sourceNode = edge.ptr->getSourceNode();
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
    currentPlan.reset(new FogTopologyPlan());
    linkID = 1;
  }

 private:
  FogTopologyManager() { currentPlan = std::make_shared<FogTopologyPlan>(); }

  FogTopologyPlanPtr currentPlan;
};
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYMANAGER_HPP_ */
