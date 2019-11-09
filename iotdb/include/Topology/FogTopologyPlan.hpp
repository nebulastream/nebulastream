#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_

#include <memory>
#include <string>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graph_traits.hpp>

#include "Topology/FogTopologyEntry.hpp"
#include "Topology/FogTopologyLink.hpp"
#include "Topology/FogTopologySensorNode.hpp"
#include "Topology/FogTopologyWorkerNode.hpp"
#include "Util/CPUCapacity.hpp"

namespace iotdb {

#define MAX_NUMBER_OF_NODES 10 // TODO: make this dynamic

struct FogVertex {
  size_t id;
  FogTopologyEntryPtr ptr;
};
struct FogEdge {
  size_t id;
  FogTopologyLinkPtr ptr;
};

class FogGraph {

  using fogGraph_t = boost::adjacency_list<boost::listS, boost::vecS, boost::undirectedS, FogVertex, FogEdge>;
  using fogVertex_t = boost::graph_traits<fogGraph_t>::vertex_descriptor;
  using fogVertex_iterator = boost::graph_traits<fogGraph_t>::vertex_iterator;
  using fogEdge_t = boost::graph_traits<fogGraph_t>::edge_descriptor;
  using fogEdge_iterator = boost::graph_traits<fogGraph_t>::edge_iterator;

 public:
  FogGraph() {};

  const fogVertex_t getVertex(size_t search_id) const;
  bool hasVertex(size_t search_id) const;

  const std::vector<FogVertex> getAllVertex() const;
  bool addVertex(FogTopologyEntryPtr ptr);
  bool removeVertex(size_t search_id);

  FogTopologyEntryPtr getRoot() const;

  FogTopologyLinkPtr getLink(FogTopologyEntryPtr sourceNode, FogTopologyEntryPtr destNode) const;
  bool hasLink(FogTopologyEntryPtr sourceNode, FogTopologyEntryPtr destNode) const;
  bool hasLink(size_t searchId) const;

  const FogTopologyLinkPtr getEdge(size_t search_id) const;
  const std::vector<FogTopologyLinkPtr> getAllEdgesToNode(FogTopologyEntryPtr destNode) const;
  const std::vector<FogTopologyLinkPtr> getAllEdgesFromNode(FogTopologyEntryPtr srcNode) const;
  const std::vector<FogTopologyLinkPtr> getAllEdges() const;
  bool hasEdge(size_t search_id) const;

  bool addEdge(FogTopologyLinkPtr ptr);
  bool removeEdge(size_t search_id);

  std::string getGraphString() const;

 private:
  fogGraph_t graph;
};

class FogTopologyPlan {

 public:
  FogTopologyPlan();

  FogTopologyEntryPtr getRootNode() const;

  FogTopologyWorkerNodePtr createFogWorkerNode(CPUCapacity cpuCapacity);
  bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr);

  FogTopologySensorNodePtr createFogSensorNode(CPUCapacity cpuCapacity);
  bool removeFogSensorNode(FogTopologySensorNodePtr ptr);

  FogTopologyLinkPtr createFogTopologyLink(FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode);
  bool removeFogTopologyLink(FogTopologyLinkPtr linkPtr);

  std::string getTopologyPlanString() const;

  FogGraph getFogGraph() const;

 private:
  size_t getNextFreeNodeId();

  size_t currentNodeId;
  size_t currentLinkId;
  FogGraph *fGraph;
  size_t getNextFreeLinkId();
};
} // namespace iotdb

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_ */
